use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, ExecutionCancelCommand, ExecutionOrderCommand, HealthSeverity,
    MarketDataChannel, MarketDataSubscription, MarketType, RequiredAccountPermission,
    RiskCapability, RiskCapabilityDeclaration, StrategyCommandSchema, StrategyConfigSchema,
    StrategyContext, StrategyEvent, StrategyHealthIssue, StrategyInstanceId, StrategyRuntime,
    StrategySnapshot, StrategySnapshotSchema, StrategySpec, StrategyStatus, TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod core;

pub use core::{
    apply_oscillation_fill, build_grid_plan, build_oscillation_grid_plan, quantize_amount,
    round_to_precision, AccountConfig, AtrConfig, BollingerConfig, ChoppinessConfig,
    ExecutionConfig, GridConfig, GridOrderPlan, GridPlan, HigherTimeframeConfig, IndicatorConfig,
    IndicatorSnapshot, LoggingConfig, LowerTimeframeConfig, MarketExitConfig, MarketRegime,
    NotificationConfig, OrderConfig, OscillationFill, OscillationFillTransition,
    OscillationGridConfig, OscillationGridCoreConfig, OscillationGridOrder, OscillationGridPlan,
    OscillationGridState, OscillationMarketCondition, OscillationOrderStatus,
    OscillationSpacingType, PairPrecision, PairRuntimeState, PrecisionManagementConfig,
    RangeGridCoreConfig, RangeRegimeClassifier, RegimeDecision, RegimeFilterConfig,
    RiskControlConfig, RsiConfig, ScheduleConfig, SessionWindow, StrategyInfo, SymbolConfig,
    SymbolPrecision, WeComConfig, WebSocketConfig,
};

pub const STRATEGY_KIND: &str = "range_grid";
pub const DISPLAY_NAME: &str = "Range Grid";
pub const MIGRATED_FROM: &str = "legacy-strategy:range_grid";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeGridStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for RangeGridStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeGridConfig {
    pub symbols: Vec<String>,
    #[serde(default = "default_exchange_id")]
    pub exchange_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    #[serde(default = "default_range_grid_candle_intervals")]
    pub candle_intervals: Vec<String>,
    pub grid_spacing_pct: f64,
    pub levels_per_side: usize,
    pub base_order_notional: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeGridSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub market_data_events: u64,
    pub execution_events: u64,
    pub account_events: u64,
    pub operator_commands: u64,
    pub timer_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_market_data_at: Option<DateTime<Utc>>,
    pub last_execution_at: Option<DateTime<Utc>>,
    pub last_account_sync_at: Option<DateTime<Utc>>,
    pub configured_symbols: Vec<String>,
    pub market_data_subscriptions: Vec<MarketDataSubscription>,
    pub last_timer_at: Option<DateTime<Utc>>,
    pub last_event_digest: Option<RuntimeEventDigest>,
    pub task_signals: Vec<RuntimeTaskSignal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventDigest {
    pub event_kind: String,
    pub symbol: Option<String>,
    pub account_id: Option<String>,
    pub client_order_id: Option<String>,
    pub command_kind: Option<String>,
    pub timer_id: Option<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTaskSignal {
    pub task_kind: String,
    pub requested_at: DateTime<Utc>,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct RangeGridRuntime {
    instance_id: StrategyInstanceId,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
    market_data_events: u64,
    execution_events: u64,
    account_events: u64,
    operator_commands: u64,
    timer_events: u64,
    last_market_data_at: Option<DateTime<Utc>>,
    last_execution_at: Option<DateTime<Utc>>,
    last_account_sync_at: Option<DateTime<Utc>>,
    config: Option<RangeGridConfig>,
    market_data_subscriptions: Vec<MarketDataSubscription>,
    last_timer_at: Option<DateTime<Utc>>,
    last_event_digest: Option<RuntimeEventDigest>,
    task_signals: Vec<RuntimeTaskSignal>,
}

impl RangeGridRuntime {
    pub fn new() -> Self {
        Self {
            instance_id: StrategyInstanceId::new("unstarted"),
            strategy_id: STRATEGY_KIND.to_string(),
            run_id: "unstarted".to_string(),
            status: StrategyStatus::Stopped,
            started_at: None,
            last_event_at: None,
            handled_events: 0,
            market_data_events: 0,
            execution_events: 0,
            account_events: 0,
            operator_commands: 0,
            timer_events: 0,
            last_market_data_at: None,
            last_execution_at: None,
            last_account_sync_at: None,
            config: None,
            market_data_subscriptions: Vec::new(),
            last_timer_at: None,
            last_event_digest: None,
            task_signals: Vec::new(),
        }
    }

    fn snapshot_payload(&self) -> RangeGridSnapshotPayload {
        RangeGridSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            market_data_events: self.market_data_events,
            execution_events: self.execution_events,
            account_events: self.account_events,
            operator_commands: self.operator_commands,
            timer_events: self.timer_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
            last_market_data_at: self.last_market_data_at,
            last_execution_at: self.last_execution_at,
            last_account_sync_at: self.last_account_sync_at,
            configured_symbols: self
                .config
                .as_ref()
                .map(|config| config.symbols.clone())
                .unwrap_or_default(),
            market_data_subscriptions: self.market_data_subscriptions.clone(),
            last_timer_at: self.last_timer_at,
            last_event_digest: self.last_event_digest.clone(),
            task_signals: self.task_signals.clone(),
        }
    }
}

impl Default for RangeGridRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for RangeGridRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        let config: RangeGridConfig = serde_json::from_value(ctx.config().clone())?;
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.market_data_subscriptions = range_grid_market_data_subscriptions(&config);
        self.config = Some(config);
        self.last_event_at = Some(ctx.started_at());
        self.handled_events = 0;
        self.market_data_events = 0;
        self.execution_events = 0;
        self.account_events = 0;
        self.operator_commands = 0;
        self.timer_events = 0;
        self.last_timer_at = None;
        self.last_event_digest = None;
        self.task_signals.clear();
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.status = StrategyStatus::Stopped;
        Ok(())
    }

    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()> {
        self.handled_events += 1;
        self.last_event_at = Some(event_timestamp(&event));
        self.last_event_digest = Some(runtime_event_digest(&event));
        for signal in runtime_task_signals(&event) {
            self.task_signals.push(signal);
        }
        if self.task_signals.len() > 16 {
            let excess = self.task_signals.len() - 16;
            self.task_signals.drain(0..excess);
        }
        match &event {
            StrategyEvent::Started(_) => self.status = StrategyStatus::Running,
            StrategyEvent::Stopping(_) => self.status = StrategyStatus::Stopping,
            StrategyEvent::Execution(event) => {
                self.execution_events += 1;
                self.last_execution_at = Some(event.occurred_at);
            }
            StrategyEvent::MarketData(event) => {
                self.market_data_events += 1;
                self.last_market_data_at = Some(event.received_at);
            }
            StrategyEvent::Account(event) => {
                self.account_events += 1;
                self.last_account_sync_at = Some(event.received_at);
            }
            StrategyEvent::OperatorCommand(command) => {
                self.operator_commands += 1;
                match command.command_kind.as_str() {
                    "pause" => self.status = StrategyStatus::Degraded,
                    "resume" => self.status = StrategyStatus::Running,
                    "stop" => self.status = StrategyStatus::Stopping,
                    _ => {}
                }
            }
            StrategyEvent::Timer(event) => {
                self.timer_events += 1;
                self.last_timer_at = Some(event.fired_at);
            }
        }
        Ok(())
    }

    async fn snapshot(&self) -> anyhow::Result<StrategySnapshot> {
        let captured_at = Utc::now();
        Ok(StrategySnapshot {
            schema_version: 1,
            instance_id: self.instance_id.clone(),
            strategy_kind: STRATEGY_KIND.to_string(),
            strategy_id: self.strategy_id.clone(),
            run_id: self.run_id.clone(),
            captured_at,
            status: self.status.clone(),
            payload: serde_json::to_value(self.snapshot_payload())?,
            health: runtime_health_issues(
                captured_at,
                &self.status,
                self.started_at,
                self.last_market_data_at,
                self.last_account_sync_at,
            ),
        })
    }
}

pub fn strategy_spec() -> StrategySpec {
    StrategySpec {
        schema_version: 1,
        strategy_kind: STRATEGY_KIND.to_string(),
        display_name: DISPLAY_NAME.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        description: Some(
            "Partially migrated range grid strategy with adapter-free classifier and planner core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places range grid limit orders",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels stale range grid orders",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Reserves per-symbol range grid inventory",
            ),
        ],
        market_data_subscriptions: Vec::new(),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadOrders,
            AccountPermission::TradeSpot,
            AccountPermission::TradePerpetual,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            ("legacy_module".to_string(), json!(MIGRATED_FROM)),
            ("partial_core_migration".to_string(), json!(true)),
            (
                "migrated_core_modules".to_string(),
                json!([
                    "core_config",
                    "indicator_snapshot",
                    "regime_classifier",
                    "grid_planner",
                    "legacy_oscillation_grid_planner",
                    "legacy_oscillation_fill_state",
                    "precision_helpers",
                    "pair_runtime_state"
                ]),
            ),
            (
                "migrated_runtime_contracts".to_string(),
                json!([
                    "sdk_context_config_loading",
                    "market_data_subscription_contract",
                    "execution_event_sync",
                    "account_event_sync",
                    "operator_command_orchestration",
                    "sanitized_runtime_snapshot"
                ]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!([
                    "controller",
                    "tasks",
                    "notifications",
                    "risk_application",
                    "precision_service",
                    "indicator_service",
                    "exchange_backed_runtime"
                ]),
            ),
            (
                "market_data_channels".to_string(),
                json!([
                    MarketDataChannel::Candles {
                        interval: "5m".to_string()
                    },
                    MarketDataChannel::Candles {
                        interval: "1h".to_string()
                    },
                    MarketDataChannel::OrderBookTop
                ]),
            ),
        ]),
    }
}

pub fn range_grid_market_data_subscriptions(
    config: &RangeGridConfig,
) -> Vec<MarketDataSubscription> {
    let channels = config
        .candle_intervals
        .iter()
        .filter(|interval| !interval.trim().is_empty())
        .map(|interval| MarketDataChannel::Candles {
            interval: interval.trim().to_string(),
        })
        .chain(std::iter::once(MarketDataChannel::OrderBookTop))
        .collect::<Vec<_>>();

    config
        .symbols
        .iter()
        .filter(|symbol| !symbol.trim().is_empty())
        .map(|symbol| MarketDataSubscription {
            exchange_id: config.exchange_id.clone(),
            symbol: symbol.trim().to_string(),
            market_type: config.market_type.clone(),
            channels: channels.clone(),
        })
        .collect()
}

pub fn range_grid_order_plan_to_execution_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    plan: &GridOrderPlan,
    requested_at: DateTime<Utc>,
) -> ExecutionOrderCommand {
    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: plan.client_id.clone(),
        idempotency_key: execution_idempotency_key(ctx, &plan.client_id),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: exchange_id.to_string(),
        symbol: symbol.to_string(),
        side: plan.side.clone(),
        order_type: plan.order_type.clone(),
        quantity: plan.quantity.to_string(),
        price: Some(plan.price.to_string()),
        time_in_force: Some(TimeInForce::GoodTilCanceled),
        reduce_only: false,
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            ("source_plan".to_string(), json!("range_grid_order_plan")),
        ]),
    }
}

pub fn range_grid_cancel_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    client_order_id: &str,
    reason: &str,
    requested_at: DateTime<Utc>,
) -> ExecutionCancelCommand {
    ExecutionCancelCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: Some(client_order_id.to_string()),
        execution_order_id: None,
        idempotency_key: execution_idempotency_key(ctx, client_order_id),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: exchange_id.to_string(),
        symbol: symbol.to_string(),
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            ("cancel_reason".to_string(), json!(reason)),
        ]),
    }
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": [
                "symbols",
                "grid_spacing_pct",
                "levels_per_side",
                "base_order_notional"
            ],
            "properties": {
                "symbols": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "exchange_id": { "type": "string", "minLength": 1, "default": "binance" },
                "market_type": {
                    "type": "string",
                    "enum": ["spot", "margin", "perpetual", "futures"],
                    "default": "spot"
                },
                "candle_intervals": {
                    "type": "array",
                    "items": { "type": "string", "minLength": 1 },
                    "default": ["5m", "1h"]
                },
                "grid_spacing_pct": { "type": "number", "exclusiveMinimum": 0.0 },
                "levels_per_side": { "type": "integer", "minimum": 1 },
                "base_order_notional": {
                    "type": "string",
                    "pattern": "^[0-9]+(\\.[0-9]+)?$"
                },
                "dry_run": { "type": "boolean", "default": true }
            }
        }),
    }
}

pub fn snapshot_schema() -> StrategySnapshotSchema {
    StrategySnapshotSchema {
        schema_version: 1,
        json_schema: common_snapshot_schema(),
    }
}

fn common_snapshot_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["migrated_from", "handled_events"],
        "properties": {
            "migrated_from": { "type": "string" },
            "handled_events": { "type": "integer", "minimum": 0 },
            "market_data_events": { "type": "integer", "minimum": 0 },
            "execution_events": { "type": "integer", "minimum": 0 },
            "account_events": { "type": "integer", "minimum": 0 },
            "operator_commands": { "type": "integer", "minimum": 0 },
            "timer_events": { "type": "integer", "minimum": 0 },
            "started_at": { "type": ["string", "null"], "format": "date-time" },
            "last_event_at": { "type": ["string", "null"], "format": "date-time" },
            "last_market_data_at": { "type": ["string", "null"], "format": "date-time" },
            "last_execution_at": { "type": ["string", "null"], "format": "date-time" },
            "last_account_sync_at": { "type": ["string", "null"], "format": "date-time" },
            "configured_symbols": {
                "type": "array",
                "items": { "type": "string" }
            },
            "market_data_subscriptions": { "type": "array" },
            "last_timer_at": { "type": ["string", "null"], "format": "date-time" },
            "last_event_digest": { "type": ["object", "null"] },
            "task_signals": { "type": "array" }
        }
    })
}

pub fn runtime_event_digest(event: &StrategyEvent) -> RuntimeEventDigest {
    match event {
        StrategyEvent::Started(event) => RuntimeEventDigest {
            event_kind: "started".to_string(),
            symbol: None,
            account_id: None,
            client_order_id: None,
            command_kind: None,
            timer_id: None,
            observed_at: event.occurred_at,
        },
        StrategyEvent::Stopping(event) => RuntimeEventDigest {
            event_kind: "stopping".to_string(),
            symbol: None,
            account_id: None,
            client_order_id: None,
            command_kind: None,
            timer_id: None,
            observed_at: event.occurred_at,
        },
        StrategyEvent::Execution(event) => RuntimeEventDigest {
            event_kind: "execution".to_string(),
            symbol: event
                .payload
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string),
            account_id: None,
            client_order_id: event.client_order_id.clone(),
            command_kind: None,
            timer_id: None,
            observed_at: event.occurred_at,
        },
        StrategyEvent::MarketData(event) => RuntimeEventDigest {
            event_kind: "market_data".to_string(),
            symbol: Some(event.symbol.clone()),
            account_id: None,
            client_order_id: None,
            command_kind: None,
            timer_id: None,
            observed_at: event.received_at,
        },
        StrategyEvent::Account(event) => RuntimeEventDigest {
            event_kind: "account".to_string(),
            symbol: event
                .payload
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string),
            account_id: Some(event.account_id.clone()),
            client_order_id: None,
            command_kind: None,
            timer_id: None,
            observed_at: event.received_at,
        },
        StrategyEvent::OperatorCommand(command) => RuntimeEventDigest {
            event_kind: "operator_command".to_string(),
            symbol: command
                .payload
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string),
            account_id: None,
            client_order_id: None,
            command_kind: Some(command.command_kind.clone()),
            timer_id: None,
            observed_at: command.requested_at,
        },
        StrategyEvent::Timer(event) => RuntimeEventDigest {
            event_kind: "timer".to_string(),
            symbol: event
                .payload
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string),
            account_id: None,
            client_order_id: None,
            command_kind: None,
            timer_id: Some(event.timer_id.clone()),
            observed_at: event.fired_at,
        },
    }
}

pub fn runtime_task_signals(event: &StrategyEvent) -> Vec<RuntimeTaskSignal> {
    match event {
        StrategyEvent::Timer(event) => vec![RuntimeTaskSignal {
            task_kind: match event.timer_id.as_str() {
                "market" | "market_tick" | "rebuild_grid" => "rebuild_grid",
                "account" | "account_sync" | "sync_positions" => "sync_account_state",
                "status" | "status_report" => "publish_status",
                other => other,
            }
            .to_string(),
            requested_at: event.fired_at,
            payload: event.payload.clone(),
        }],
        StrategyEvent::OperatorCommand(command)
            if matches!(command.command_kind.as_str(), "rebuild_grid" | "resume") =>
        {
            vec![RuntimeTaskSignal {
                task_kind: command.command_kind.clone(),
                requested_at: command.requested_at,
                payload: command.payload.clone(),
            }]
        }
        _ => Vec::new(),
    }
}

pub fn runtime_health_issues(
    now: DateTime<Utc>,
    status: &StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_market_data_at: Option<DateTime<Utc>>,
    last_account_sync_at: Option<DateTime<Utc>>,
) -> Vec<StrategyHealthIssue> {
    if !matches!(status, StrategyStatus::Running | StrategyStatus::Degraded) {
        return Vec::new();
    }
    let mut issues = Vec::new();
    push_staleness_issue(
        &mut issues,
        now,
        started_at,
        last_market_data_at,
        "market_data_stale",
        "No recent market data event observed",
        300,
    );
    push_staleness_issue(
        &mut issues,
        now,
        started_at,
        last_account_sync_at,
        "account_sync_stale",
        "No recent account sync event observed",
        900,
    );
    issues
}

fn push_staleness_issue(
    issues: &mut Vec<StrategyHealthIssue>,
    now: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    last_seen_at: Option<DateTime<Utc>>,
    issue_kind: &str,
    message: &str,
    threshold_secs: i64,
) {
    let Some(reference) = last_seen_at.or(started_at) else {
        return;
    };
    let age_secs = now.signed_duration_since(reference).num_seconds().max(0);
    if age_secs <= threshold_secs {
        return;
    }
    issues.push(StrategyHealthIssue {
        severity: HealthSeverity::Warning,
        message: message.to_string(),
        observed_at: now,
        details: Some(json!({
            "issue_kind": issue_kind,
            "age_secs": age_secs,
            "threshold_secs": threshold_secs,
        })),
    });
}

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    ["pause", "resume", "stop", "rebuild_grid"]
        .into_iter()
        .map(|command_kind| StrategyCommandSchema {
            command_kind: command_kind.to_string(),
            description: Some(format!("Range grid runtime {command_kind} command")),
            payload_schema: json!({
                "type": "object",
                "additionalProperties": true
            }),
        })
        .collect()
}

fn default_exchange_id() -> String {
    "binance".to_string()
}

fn default_market_type() -> MarketType {
    MarketType::Spot
}

fn default_range_grid_candle_intervals() -> Vec<String> {
    ["5m", "1h"].into_iter().map(str::to_string).collect()
}

fn execution_idempotency_key(ctx: &StrategyContext, client_order_id: &str) -> String {
    format!("{}:{}:{}", ctx.strategy_id(), ctx.run_id(), client_order_id)
}

fn event_timestamp(event: &StrategyEvent) -> DateTime<Utc> {
    match event {
        StrategyEvent::Started(event) | StrategyEvent::Stopping(event) => event.occurred_at,
        StrategyEvent::Execution(event) => event.occurred_at,
        StrategyEvent::MarketData(event) => event.received_at,
        StrategyEvent::Account(event) => event.received_at,
        StrategyEvent::OperatorCommand(command) => command.requested_at,
        StrategyEvent::Timer(event) => event.fired_at,
    }
}

fn risk_capability(
    capability: RiskCapability,
    description: impl Into<String>,
) -> RiskCapabilityDeclaration {
    RiskCapabilityDeclaration {
        capability,
        description: Some(description.into()),
        limits: json!({ "configured_per_instance": true }),
    }
}

fn account_permissions(permissions: &[AccountPermission]) -> Vec<RequiredAccountPermission> {
    permissions
        .iter()
        .cloned()
        .map(|permission| RequiredAccountPermission {
            account_id: None,
            permission,
            reason: Some("Required by configured strategy runtime".to_string()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rustcta_strategy_sdk::{
        AccountEvent, ExecutionCancelAck, ExecutionCancelCommand, ExecutionEvent, ExecutionIntent,
        ExecutionIntentAck, ExecutionOrderAck, ExecutionOrderCommand, MarketDataEvent, OrderSide,
        OrderType, SdkResult, StrategyCommand, StrategyExecutionClient, TimeInForce, TimerEvent,
    };
    use std::sync::Arc;

    struct NoopExecutionClient;

    #[async_trait]
    impl StrategyExecutionClient for NoopExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<ExecutionOrderAck> {
            Ok(ExecutionOrderAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: None,
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
                accepted: true,
                intent_kind: intent.intent_kind,
                reason: None,
                received_at: Utc::now(),
                payload: Value::Null,
            })
        }
    }

    #[test]
    fn spec_should_expose_config_and_snapshot_schemas() {
        let spec = strategy_spec();
        assert_eq!(spec.strategy_kind, STRATEGY_KIND);
        assert_eq!(spec.config_schema.schema_version, 1);
        assert_eq!(spec.config_schema.json_schema["type"], json!("object"));
        assert!(spec.config_schema.json_schema["required"]
            .as_array()
            .is_some_and(|required| !required.is_empty()));
        assert!(spec
            .supported_commands
            .iter()
            .any(|command| command.command_kind == "rebuild_grid"));
        assert_eq!(spec.snapshot_schema.schema_version, 1);
        assert_eq!(spec.snapshot_schema.json_schema["type"], json!("object"));
        assert!(spec.snapshot_schema.json_schema["properties"]["handled_events"].is_object());
        assert!(spec.snapshot_schema.json_schema["properties"]["market_data_events"].is_object());
        assert_secret_free(&serde_json::to_value(spec).expect("spec should serialize"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_should_be_secret_free() {
        let mut runtime = RangeGridRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({
                "api_key": "must-not-leak",
                "secret": "must-not-leak",
                "symbols": ["BTC/USDT"],
                "grid_spacing_pct": 0.5,
                "levels_per_side": 3,
                "base_order_notional": "50"
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        let snapshot = runtime.snapshot().await.expect("snapshot should build");

        assert_eq!(snapshot.strategy_kind, STRATEGY_KIND);
        assert_eq!(snapshot.status, StrategyStatus::Running);
        assert_secret_free(&snapshot.payload);
        assert_secret_free(&serde_json::to_value(snapshot).expect("snapshot should serialize"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_contract_should_subscribe_and_sync_events() {
        let mut runtime = RangeGridRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({
                "symbols": ["BTC/USDT", "ETH/USDT"],
                "exchange_id": "paper",
                "market_type": "spot",
                "candle_intervals": ["5m", "1h"],
                "grid_spacing_pct": 0.5,
                "levels_per_side": 3,
                "base_order_notional": "50"
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        assert_eq!(runtime.market_data_subscriptions.len(), 2);
        assert_eq!(runtime.market_data_subscriptions[0].exchange_id, "paper");
        assert_eq!(
            runtime.market_data_subscriptions[0].market_type,
            MarketType::Spot
        );
        assert!(runtime.market_data_subscriptions[0]
            .channels
            .contains(&MarketDataChannel::OrderBookTop));

        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap();
        runtime
            .handle_event(StrategyEvent::MarketData(MarketDataEvent {
                schema_version: 1,
                exchange_id: "paper".to_string(),
                symbol: "BTC/USDT".to_string(),
                received_at: observed_at,
                payload: json!({"close": "100.0"}),
            }))
            .await
            .expect("market event should apply");
        runtime
            .handle_event(StrategyEvent::Execution(ExecutionEvent {
                schema_version: 1,
                event_id: "exec-1".to_string(),
                client_order_id: Some("rg-btc-1".to_string()),
                occurred_at: observed_at,
                payload: json!({"status": "filled"}),
            }))
            .await
            .expect("execution event should apply");
        runtime
            .handle_event(StrategyEvent::Account(AccountEvent {
                schema_version: 1,
                account_id: "account-1".to_string(),
                received_at: observed_at,
                payload: json!({"balances": []}),
            }))
            .await
            .expect("account event should apply");
        runtime
            .handle_event(StrategyEvent::OperatorCommand(StrategyCommand {
                schema_version: 1,
                command_id: "cmd-1".to_string(),
                instance_id: StrategyInstanceId::new("instance-1"),
                command_kind: "pause".to_string(),
                requested_at: observed_at,
                payload: Value::Null,
                requested_by: Some("test".to_string()),
            }))
            .await
            .expect("command should apply");

        let snapshot = runtime.snapshot().await.expect("snapshot should build");
        assert_eq!(snapshot.status, StrategyStatus::Degraded);
        assert_eq!(snapshot.payload["market_data_events"], json!(1));
        assert_eq!(snapshot.payload["execution_events"], json!(1));
        assert_eq!(snapshot.payload["account_events"], json!(1));
        assert_eq!(snapshot.payload["operator_commands"], json!(1));
        assert_eq!(snapshot.payload["configured_symbols"][0], json!("BTC/USDT"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_should_digest_timer_tasks_and_report_stale_health() {
        let mut runtime = RangeGridRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "range-grid",
            "run-1",
            json!({
                "symbols": ["BTC/USDT"],
                "grid_spacing_pct": 0.5,
                "levels_per_side": 3,
                "base_order_notional": "50"
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        let fired_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 2, 0).unwrap();
        runtime
            .handle_event(StrategyEvent::Timer(TimerEvent {
                schema_version: 1,
                timer_id: "market_tick".to_string(),
                fired_at,
                payload: json!({"symbol": "BTC/USDT"}),
            }))
            .await
            .expect("timer event should apply");

        let snapshot = runtime.snapshot().await.expect("snapshot should build");
        assert_eq!(snapshot.payload["timer_events"], json!(1));
        assert_eq!(
            snapshot.payload["last_event_digest"]["event_kind"],
            json!("timer")
        );
        assert_eq!(
            snapshot.payload["task_signals"][0]["task_kind"],
            json!("rebuild_grid")
        );

        let stale = runtime_health_issues(
            fired_at + chrono::Duration::seconds(901),
            &StrategyStatus::Running,
            Some(fired_at),
            None,
            None,
        );
        assert_eq!(stale.len(), 2);
        assert_eq!(stale[0].severity, HealthSeverity::Warning);
        assert_eq!(
            stale[0].details.as_ref().unwrap()["issue_kind"],
            json!("market_data_stale")
        );
    }

    #[test]
    fn grid_order_plan_should_map_to_sdk_execution_commands() {
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "range-grid",
            "run-1",
            json!({
                "symbols": ["BTC/USDT"],
                "grid_spacing_pct": 0.5,
                "levels_per_side": 3,
                "base_order_notional": "50"
            }),
            execution,
        );
        let requested_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 2, 0).unwrap();
        let plan = GridOrderPlan {
            client_id: "rg-btc-buy-1".to_string(),
            price: 99.5,
            quantity: 0.5,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
        };

        let order = range_grid_order_plan_to_execution_command(
            &ctx,
            "paper",
            "BTC/USDT",
            "risk-range",
            &plan,
            requested_at,
        );

        assert_eq!(order.client_order_id, "rg-btc-buy-1");
        assert_eq!(order.exchange_id, "paper");
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.time_in_force, Some(TimeInForce::GoodTilCanceled));
        assert!(!order.reduce_only);
        assert_eq!(order.quantity, "0.5");
        assert_eq!(order.price, Some("99.5".to_string()));
        assert_eq!(
            order.metadata["source_plan"],
            json!("range_grid_order_plan")
        );

        let cancel = range_grid_cancel_command(
            &ctx,
            "paper",
            "BTC/USDT",
            "risk-range",
            "rg-btc-buy-1",
            "regime_changed",
            requested_at,
        );
        assert_eq!(cancel.client_order_id, Some("rg-btc-buy-1".to_string()));
        assert_eq!(cancel.execution_order_id, None);
        assert_eq!(cancel.metadata["cancel_reason"], json!("regime_changed"));
    }

    #[test]
    fn manifest_should_not_depend_on_exchange_adapters() {
        let manifest = include_str!("../Cargo.toml");
        assert!(manifest.contains("rustcta-strategy-sdk.workspace = true"));
        for forbidden in [
            "rustcta-exchange-api",
            "rustcta-exchange-gateway",
            "legacy exchange adapter path",
            "gateio",
            "kucoin",
            "okx",
            "binance",
            "bitget",
            "mexc",
        ] {
            assert!(
                !manifest.contains(forbidden),
                "{forbidden} should not appear in manifest"
            );
        }
    }

    #[test]
    fn migrated_core_should_classify_range_regime() {
        let symbol_cfg = sample_symbol(3);
        let indicator_cfg = sample_indicator_config();
        let classifier = RangeRegimeClassifier::new(&symbol_cfg, &indicator_cfg);
        let snapshot = IndicatorSnapshot {
            timestamp: Utc::now(),
            last_price: 100.0,
            lower_band_ratio: 0.5,
            z_score: 0.1,
            rsi: 50.0,
            atr: 1.5,
            adx: 15.0,
            bbw_percentile: 0.2,
            slope_metric: 0.05,
            choppiness: Some(70.0),
        };

        let decision = classifier.evaluate(&snapshot, MarketRegime::TrendUp, false);

        assert_eq!(decision.new_regime, MarketRegime::Range);
        assert!(decision.activate_grid);
        assert!(!decision.deactivate_grid);
        assert!(decision.reason.expect("reason").contains("RSI"));
    }

    #[test]
    fn migrated_core_should_classify_trend_regime() {
        let symbol_cfg = sample_symbol(2);
        let indicator_cfg = sample_indicator_config();
        let classifier = RangeRegimeClassifier::new(&symbol_cfg, &indicator_cfg);
        let snapshot = IndicatorSnapshot {
            timestamp: Utc::now(),
            last_price: 100.0,
            lower_band_ratio: 0.9,
            z_score: 3.0,
            rsi: 80.0,
            atr: 3.5,
            adx: 40.0,
            bbw_percentile: 0.8,
            slope_metric: 0.6,
            choppiness: Some(20.0),
        };

        let decision = classifier.evaluate(&snapshot, MarketRegime::Range, true);

        assert_ne!(decision.new_regime, MarketRegime::Range);
        assert!(decision.deactivate_grid);
        assert!(!decision.activate_grid);
    }

    #[test]
    fn migrated_core_should_build_grid_plan_levels() {
        let symbol = sample_symbol(2);
        let precision = PairPrecision {
            price_digits: 2,
            amount_digits: 3,
            price_step: 0.01,
            amount_step: 0.001,
            min_notional: Some(5.0),
        };

        let plan = build_grid_plan(&symbol, &precision, 50_000.0).expect("plan should build");
        let buy_orders = plan
            .orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .collect::<Vec<_>>();
        let sell_orders = plan
            .orders
            .iter()
            .filter(|order| order.side == OrderSide::Sell)
            .collect::<Vec<_>>();

        assert_eq!(plan.orders.len(), 4);
        assert_eq!(buy_orders.len(), 2);
        assert_eq!(sell_orders.len(), 2);
        assert!(buy_orders[0].price < 50_000.0);
        assert!(sell_orders[0].price > 50_000.0);
        assert!(buy_orders[0].quantity * buy_orders[0].price >= 5.0);
        assert!(buy_orders[0]
            .client_id
            .starts_with("range_grid_paper_btcusdt_B_"));
    }

    #[test]
    fn migrated_core_should_cap_order_notional_by_position_limit() {
        let mut symbol = sample_symbol(2);
        symbol.grid.base_order_notional = 200.0;
        symbol.grid.max_position_notional = 100.0;
        let precision = PairPrecision {
            price_digits: 2,
            amount_digits: 3,
            price_step: 0.01,
            amount_step: 0.001,
            min_notional: None,
        };

        let plan = build_grid_plan(&symbol, &precision, 100.0).expect("plan should build");

        assert_eq!(plan.orders.len(), 4);
        assert!(plan
            .orders
            .iter()
            .all(|order| order.price * order.quantity <= 50.1));
    }

    #[test]
    fn migrated_core_should_track_pair_runtime_state() {
        let symbol = sample_symbol(2);
        let mut state = PairRuntimeState::new(symbol);
        let precision = PairPrecision {
            price_digits: 2,
            amount_digits: 3,
            price_step: 0.01,
            amount_step: 0.001,
            min_notional: Some(5.0),
        };
        let indicator = IndicatorSnapshot {
            last_price: 123.45,
            ..IndicatorSnapshot::default()
        };

        state.mark_precision(precision);
        state.mark_indicator(indicator);
        state.mark_regime(MarketRegime::Range);
        state.activate_grid(GridPlan {
            center_price: 123.45,
            orders: Vec::new(),
        });

        assert_eq!(state.current_price, 123.45);
        assert_eq!(state.regime, MarketRegime::Range);
        assert!(state.grid_active);
        assert!(state.started_at.is_some());
        state.deactivate_grid();
        assert!(!state.grid_active);
        assert!(state.current_plan.is_none());
    }

    #[test]
    fn migrated_oscillation_core_should_build_arithmetic_fixed_grid() {
        let config = sample_oscillation_config(OscillationSpacingType::Arithmetic);

        let plan = build_oscillation_grid_plan(&config, 100.0).expect("plan should build");
        let buys = plan
            .orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .collect::<Vec<_>>();
        let sells = plan
            .orders
            .iter()
            .filter(|order| order.side == OrderSide::Sell)
            .collect::<Vec<_>>();

        assert_eq!(plan.orders.len(), 4);
        assert_eq!(buys[0].price, 99.0);
        assert_eq!(buys[1].price, 98.0);
        assert_eq!(sells[0].price, 101.0);
        assert_eq!(sells[1].price, 102.0);
        assert!((buys[0].quantity - 10.0 / 99.0).abs() < 1e-12);
        assert!(buys[0].client_id.starts_with("osc_paper_btcusdt_B_"));
    }

    #[test]
    fn migrated_oscillation_core_should_build_geometric_fixed_grid() {
        let config = sample_oscillation_config(OscillationSpacingType::Geometric);

        let plan = build_oscillation_grid_plan(&config, 100.0).expect("plan should build");
        let buy_second = plan
            .orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .nth(1)
            .expect("second buy");
        let sell_second = plan
            .orders
            .iter()
            .filter(|order| order.side == OrderSide::Sell)
            .nth(1)
            .expect("second sell");

        assert!((buy_second.price - 100.0 * 0.99_f64.powi(2)).abs() < 1e-12);
        assert!((sell_second.price - 100.0 * 1.01_f64.powi(2)).abs() < 1e-12);
    }

    #[test]
    fn migrated_oscillation_core_should_update_state_and_replenish_after_fills() {
        let config = sample_oscillation_config(OscillationSpacingType::Arithmetic);
        let plan = build_oscillation_grid_plan(&config, 100.0).expect("plan should build");
        let mut state = OscillationGridState::new(100.0);
        state.activate_with_orders(plan, Utc::now());
        let buy_order_id = state.buy_orders[0].order_id.clone();
        let sell_order_id = state.sell_orders[0].order_id.clone();

        let buy_transition = apply_oscillation_fill(
            &config,
            &mut state,
            OscillationFill {
                order_id: buy_order_id,
                side: OrderSide::Buy,
                filled_quantity: 0.1,
                fill_price: 99.0,
            },
        )
        .expect("buy fill should apply");

        assert_eq!(state.total_trades, 1);
        assert!((state.position_quantity - 0.1).abs() < 1e-12);
        assert_eq!(buy_transition.realized_profit_delta, 0.0);
        assert!((buy_transition.replenish_order.price - 98.01).abs() < 1e-12);

        let sell_transition = apply_oscillation_fill(
            &config,
            &mut state,
            OscillationFill {
                order_id: sell_order_id,
                side: OrderSide::Sell,
                filled_quantity: 0.05,
                fill_price: 101.0,
            },
        )
        .expect("sell fill should apply");

        assert_eq!(state.total_trades, 2);
        assert!((state.position_quantity - 0.05).abs() < 1e-12);
        assert!((sell_transition.realized_profit_delta - 0.05 * 0.01 * 101.0).abs() < 1e-12);
        assert!((state.total_profit - sell_transition.realized_profit_delta).abs() < 1e-12);
        assert!((sell_transition.replenish_order.price - 102.01).abs() < 1e-12);
    }

    fn assert_secret_free(value: &Value) {
        let encoded = value.to_string().to_ascii_lowercase();
        for forbidden in [
            "api_key",
            "secret",
            "passphrase",
            "password",
            "token",
            "credential",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "{forbidden} should not appear in serialized runtime output"
            );
        }
    }

    fn sample_symbol(require_conditions: usize) -> SymbolConfig {
        SymbolConfig {
            config_id: "TEST".to_string(),
            enabled: true,
            account: AccountConfig {
                id: "acc".to_string(),
                exchange: "paper".to_string(),
                env_prefix: "PAPER".to_string(),
            },
            symbol: "BTCUSDT".to_string(),
            precision: SymbolPrecision {
                price_digits: Some(2),
                amount_digits: Some(3),
            },
            grid: GridConfig {
                grid_spacing_pct: 0.5,
                levels_per_side: 2,
                base_order_notional: 100.0,
                max_position_notional: 400.0,
            },
            regime_filters: RegimeFilterConfig {
                min_liquidity_usd: 1_000_000.0,
                require_conditions,
                adx_max: 25.0,
                bbw_quantile: 0.5,
                slope_threshold: 0.2,
            },
            order: OrderConfig {
                post_only: true,
                tif: Some("GTC".to_string()),
            },
            market_exit: MarketExitConfig {
                use_market_order: true,
            },
        }
    }

    fn sample_indicator_config() -> IndicatorConfig {
        IndicatorConfig {
            lower_timeframe: LowerTimeframeConfig {
                timeframe: "5m".to_string(),
                bollinger: BollingerConfig { length: 20, k: 2.0 },
                rsi: RsiConfig {
                    length: 14,
                    overbought: 70.0,
                    oversold: 30.0,
                },
                atr: AtrConfig {
                    length: 14,
                    stop_multiplier: 2.5,
                    trail_multiplier: 1.5,
                },
            },
            higher_timeframe: HigherTimeframeConfig {
                timeframe: "15m".to_string(),
                adx_length: 14,
                adx_threshold: 20.0,
                bbw_window: 400,
                bbw_quantile: 0.35,
                slope_threshold: 0.1,
                choppiness: None,
            },
        }
    }

    fn sample_oscillation_config(
        spacing_type: OscillationSpacingType,
    ) -> OscillationGridCoreConfig {
        OscillationGridCoreConfig {
            config_id: "OSC".to_string(),
            enabled: true,
            account: AccountConfig {
                id: "acc".to_string(),
                exchange: "paper".to_string(),
                env_prefix: "PAPER".to_string(),
            },
            symbol: "BTCUSDT".to_string(),
            grid: OscillationGridConfig {
                spacing: 0.01,
                spacing_type,
                order_amount: 10.0,
                orders_per_side: 2,
                max_position: 100.0,
            },
            market_condition: OscillationMarketCondition {
                max_trend_score: 20.0,
                min_volatility: 0.001,
                max_volatility: 0.05,
                check_interval: 60,
                auto_stop: true,
                stop_loss_ratio: 0.1,
            },
        }
    }
}
