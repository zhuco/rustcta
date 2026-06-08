use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, ExecutionCancelCommand, ExecutionOrderCommand, HealthSeverity,
    MarketDataChannel, MarketDataSubscription, MarketType, OrderType, RequiredAccountPermission,
    RiskCapability, RiskCapabilityDeclaration, StrategyCommandSchema, StrategyConfigSchema,
    StrategyContext, StrategyEvent, StrategyHealthIssue, StrategyInstanceId, StrategyRuntime,
    StrategySnapshot, StrategySnapshotSchema, StrategySpec, StrategyStatus, TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod core;

pub use core::{
    adjust_post_only_price, build_market_streams, build_order_plan, build_risk_limits,
    build_risk_snapshot, calculate_market_activity_factor, calculate_optimal_spread,
    calculate_rates, close_order_quantity, estimate_poisson_parameters, evaluate_risk,
    filled_intent_cancels_counterpart, is_order_missing_error, is_post_only_reject,
    is_reduce_only_rejection, max_inventory_quantity, quantity_step_size, round_price,
    round_price_for_side, round_quantity, should_refresh_orders, tick_size,
    update_poisson_params_on_fill, CancelDraft, CancelReason, LocalOrderBook, MMStrategyState,
    OrderDraft, OrderEventType, OrderFlowEvent, OrderIntent, OrderSlotInfo, PoissonAccountConfig,
    PoissonMMConfig, PoissonModelConfig, PoissonOrderPlan, PoissonOrderRecord, PoissonOrderStatus,
    PoissonParameters, PoissonRiskAction, PoissonRiskConfig, PoissonRiskLimits,
    PoissonRiskSnapshot, PoissonTradingConfig, PriceRoundingSide, SpreadQuote, SymbolInfo,
};

pub const STRATEGY_KIND: &str = "poisson_market_maker";
pub const DISPLAY_NAME: &str = "Poisson Market Maker";
pub const MIGRATED_FROM: &str = "legacy-strategy:poisson_market_maker";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoissonMarketMakerStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for PoissonMarketMakerStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonMarketMakerConfig {
    pub symbol: String,
    #[serde(default = "default_exchange_id")]
    pub exchange_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    pub order_size_usdc: String,
    pub max_inventory: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoissonMarketMakerSnapshotPayload {
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
    pub configured_symbol: Option<String>,
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
pub struct PoissonMarketMakerRuntime {
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
    config: Option<PoissonMarketMakerConfig>,
    market_data_subscriptions: Vec<MarketDataSubscription>,
    last_timer_at: Option<DateTime<Utc>>,
    last_event_digest: Option<RuntimeEventDigest>,
    task_signals: Vec<RuntimeTaskSignal>,
}

impl PoissonMarketMakerRuntime {
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

    fn snapshot_payload(&self) -> PoissonMarketMakerSnapshotPayload {
        PoissonMarketMakerSnapshotPayload {
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
            configured_symbol: self.config.as_ref().map(|config| config.symbol.clone()),
            market_data_subscriptions: self.market_data_subscriptions.clone(),
            last_timer_at: self.last_timer_at,
            last_event_digest: self.last_event_digest.clone(),
            task_signals: self.task_signals.clone(),
        }
    }
}

impl Default for PoissonMarketMakerRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for PoissonMarketMakerRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        let config: PoissonMarketMakerConfig = serde_json::from_value(ctx.config().clone())?;
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.market_data_subscriptions = poisson_market_data_subscriptions(&config);
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
            "Partially migrated Poisson queue market-making strategy with adapter-free core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places queue-model market-making limit orders",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels stale or replaced quote orders",
            ),
            risk_capability(
                RiskCapability::ReduceOnlyOrders,
                "Reduces long or short inventory through close intents",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Controls gross and net inventory against configured notional caps",
            ),
        ],
        market_data_subscriptions: Vec::new(),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadPositions,
            AccountPermission::ReadOrders,
            AccountPermission::ReadFills,
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
                    "order_flow_dto",
                    "poisson_parameters",
                    "order_slot_state",
                    "precision_rules",
                    "refresh_decision",
                    "activity_factor",
                    "optimal_spread",
                    "order_plan",
                    "risk_limits",
                    "risk_snapshot",
                    "risk_action_classifier",
                    "post_only_adjustment",
                    "error_classifiers",
                    "market_stream_names"
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
                    "exchange_io",
                    "real_order_lifecycle",
                    "risk_evaluator_wiring",
                    "webhook_notifications",
                    "trade_collector",
                    "exchange_backed_runtime"
                ]),
            ),
            (
                "market_data_channels".to_string(),
                json!([MarketDataChannel::OrderBookDepth, MarketDataChannel::Trades]),
            ),
            (
                "primary_market_type".to_string(),
                json!(MarketType::Perpetual),
            ),
        ]),
    }
}

pub fn poisson_market_data_subscriptions(
    config: &PoissonMarketMakerConfig,
) -> Vec<MarketDataSubscription> {
    if config.symbol.trim().is_empty() {
        return Vec::new();
    }

    vec![MarketDataSubscription {
        exchange_id: config.exchange_id.clone(),
        symbol: config.symbol.trim().to_string(),
        market_type: config.market_type.clone(),
        channels: vec![MarketDataChannel::OrderBookDepth, MarketDataChannel::Trades],
    }]
}

pub fn poisson_order_draft_to_execution_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    client_order_id: &str,
    draft: &OrderDraft,
    requested_at: DateTime<Utc>,
) -> ExecutionOrderCommand {
    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: client_order_id.to_string(),
        idempotency_key: execution_idempotency_key(ctx, client_order_id),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: exchange_id.to_string(),
        symbol: symbol.to_string(),
        side: draft.side.clone(),
        order_type: if draft.post_only {
            OrderType::PostOnly
        } else {
            OrderType::Limit
        },
        quantity: draft.quantity.to_string(),
        price: Some(draft.price.to_string()),
        time_in_force: Some(if draft.post_only {
            TimeInForce::PostOnly
        } else {
            TimeInForce::GoodTilCanceled
        }),
        reduce_only: draft.reduce_only,
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            ("source_plan".to_string(), json!("poisson_order_draft")),
            ("intent".to_string(), json!(draft.intent)),
            ("post_only".to_string(), json!(draft.post_only)),
        ]),
    }
}

pub fn poisson_cancel_draft_to_execution_command(
    ctx: &StrategyContext,
    venue_exchange_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    draft: &CancelDraft,
    requested_at: DateTime<Utc>,
) -> ExecutionCancelCommand {
    let key_source = draft.client_id.as_deref().unwrap_or(&draft.exchange_id);
    ExecutionCancelCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: draft.client_id.clone(),
        execution_order_id: Some(draft.exchange_id.clone()),
        idempotency_key: execution_idempotency_key(ctx, key_source),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: venue_exchange_id.to_string(),
        symbol: symbol.to_string(),
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            ("intent".to_string(), json!(draft.intent)),
            ("cancel_reason".to_string(), json!(draft.reason)),
        ]),
    }
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["symbol", "order_size_usdc", "max_inventory"],
            "properties": {
                "symbol": { "type": "string", "minLength": 1 },
                "exchange_id": { "type": "string", "minLength": 1, "default": "binance" },
                "market_type": {
                    "type": "string",
                    "enum": ["spot", "margin", "perpetual", "futures"],
                    "default": "perpetual"
                },
                "order_size_usdc": {
                    "type": "string",
                    "pattern": "^[0-9]+(\\.[0-9]+)?$"
                },
                "max_inventory": {
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
        json_schema: json!({
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
                "configured_symbol": { "type": ["string", "null"] },
                "market_data_subscriptions": { "type": "array" },
                "last_timer_at": { "type": ["string", "null"], "format": "date-time" },
                "last_event_digest": { "type": ["object", "null"] },
                "task_signals": { "type": "array" }
            }
        }),
    }
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
                "market" | "market_tick" | "refresh_quotes" => "refresh_quotes",
                "account" | "account_sync" | "sync_positions" => "sync_account_state",
                "status" | "status_report" => "publish_status",
                other => other,
            }
            .to_string(),
            requested_at: event.fired_at,
            payload: event.payload.clone(),
        }],
        StrategyEvent::OperatorCommand(command)
            if matches!(command.command_kind.as_str(), "refresh_quotes" | "resume") =>
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

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    ["pause", "resume", "stop", "refresh_quotes"]
        .into_iter()
        .map(|command_kind| StrategyCommandSchema {
            command_kind: command_kind.to_string(),
            description: Some(format!(
                "Poisson market-maker runtime {command_kind} command"
            )),
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
    MarketType::Perpetual
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
    use chrono::{Duration, TimeZone};
    use rustcta_strategy_sdk::{
        AccountEvent, ExecutionCancelAck, ExecutionCancelCommand, ExecutionEvent, ExecutionIntent,
        ExecutionIntentAck, ExecutionOrderAck, ExecutionOrderCommand, MarketDataEvent, OrderSide,
        SdkResult, StrategyCommand, StrategyExecutionClient, TimerEvent,
    };
    use serde_json::Value;
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
        assert!(spec
            .supported_commands
            .iter()
            .any(|command| command.command_kind == "refresh_quotes"));
        assert_eq!(spec.snapshot_schema.schema_version, 1);
        assert_eq!(spec.snapshot_schema.json_schema["type"], json!("object"));
        assert!(spec.snapshot_schema.json_schema["properties"]["market_data_events"].is_object());
        assert_secret_free(&serde_json::to_value(spec).expect("spec should serialize"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_should_be_secret_free() {
        let mut runtime = PoissonMarketMakerRuntime::new();
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
                "symbol": "NEAR/USDC",
                "order_size_usdc": "10",
                "max_inventory": "100"
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
        let mut runtime = PoissonMarketMakerRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({
                "symbol": "NEAR/USDC",
                "exchange_id": "paper",
                "market_type": "perpetual",
                "order_size_usdc": "10",
                "max_inventory": "100"
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        assert_eq!(runtime.market_data_subscriptions.len(), 1);
        assert_eq!(runtime.market_data_subscriptions[0].exchange_id, "paper");
        assert!(runtime.market_data_subscriptions[0]
            .channels
            .contains(&MarketDataChannel::OrderBookDepth));
        assert!(runtime.market_data_subscriptions[0]
            .channels
            .contains(&MarketDataChannel::Trades));

        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap();
        runtime
            .handle_event(StrategyEvent::MarketData(MarketDataEvent {
                schema_version: 1,
                exchange_id: "paper".to_string(),
                symbol: "NEAR/USDC".to_string(),
                received_at: observed_at,
                payload: json!({"bid": "99.9", "ask": "100.1"}),
            }))
            .await
            .expect("market event should apply");
        runtime
            .handle_event(StrategyEvent::Execution(ExecutionEvent {
                schema_version: 1,
                event_id: "exec-1".to_string(),
                client_order_id: Some("pmm-near-1".to_string()),
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
                payload: json!({"positions": []}),
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
        assert_eq!(snapshot.payload["configured_symbol"], json!("NEAR/USDC"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_should_digest_timer_tasks_and_report_stale_health() {
        let mut runtime = PoissonMarketMakerRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "poisson-mm",
            "run-1",
            json!({"symbol": "NEAR/USDC", "order_size_usdc": "10", "max_inventory": "100"}),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        let fired_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 2, 0).unwrap();
        runtime
            .handle_event(StrategyEvent::Timer(TimerEvent {
                schema_version: 1,
                timer_id: "market_tick".to_string(),
                fired_at,
                payload: json!({"symbol": "NEAR/USDC"}),
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
            json!("refresh_quotes")
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
    fn order_drafts_should_map_to_sdk_execution_commands() {
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "poisson-mm",
            "run-1",
            json!({"symbol": "NEAR/USDC", "order_size_usdc": "10", "max_inventory": "100"}),
            execution,
        );
        let requested_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 2, 0).unwrap();
        let draft = OrderDraft {
            intent: OrderIntent::CloseShort,
            side: OrderSide::Buy,
            price: 99.5,
            quantity: 2.0,
            post_only: false,
            reduce_only: true,
        };

        let order = poisson_order_draft_to_execution_command(
            &ctx,
            "paper",
            "NEAR/USDC",
            "risk-mm",
            "pmm-near-close-short",
            &draft,
            requested_at,
        );

        assert_eq!(order.exchange_id, "paper");
        assert_eq!(order.symbol, "NEAR/USDC");
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.time_in_force, Some(TimeInForce::GoodTilCanceled));
        assert!(order.reduce_only);
        assert_eq!(order.quantity, "2");
        assert_eq!(order.price, Some("99.5".to_string()));
        assert_eq!(order.metadata["intent"], json!("close_short"));

        let cancel = CancelDraft {
            intent: OrderIntent::OpenLong,
            exchange_id: "venue-order-1".to_string(),
            client_id: Some("pmm-near-open-long".to_string()),
            reason: CancelReason::ReplacePrice,
        };
        let command = poisson_cancel_draft_to_execution_command(
            &ctx,
            "paper",
            "NEAR/USDC",
            "risk-mm",
            &cancel,
            requested_at,
        );

        assert_eq!(
            command.client_order_id,
            Some("pmm-near-open-long".to_string())
        );
        assert_eq!(
            command.execution_order_id,
            Some("venue-order-1".to_string())
        );
        assert_eq!(command.metadata["cancel_reason"], json!("replace_price"));
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
    fn migrated_config_should_parse_and_validate_shape() {
        let config: PoissonMMConfig = serde_yaml::from_str(CONFIG_YAML).expect("config parses");
        assert_eq!(config.trading.symbol, "NEAR/USDC");
        assert_eq!(config.poisson.initial_lambda, 3.0);
        assert_secret_free(&serde_json::to_value(config).expect("config serializes"));
    }

    #[test]
    fn initial_poisson_params_use_lambda_and_stable_queue_formula() {
        let now = Utc::now();
        let params = PoissonParameters::from_initial_lambda(3.0, now);

        assert_eq!(params.lambda_bid, 3.0);
        assert!((params.mu_bid - 3.6).abs() < 1e-9);
        assert!((params.avg_queue_bid - 5.0).abs() < 1e-9);
        assert_eq!(params.last_update, now);
    }

    #[test]
    fn calculate_rates_counts_only_trade_events() {
        let now = Utc::now();
        let events = vec![
            flow(now, OrderSide::Buy, OrderEventType::NewOrder),
            flow(
                now + Duration::seconds(5),
                OrderSide::Buy,
                OrderEventType::Trade,
            ),
            flow(
                now + Duration::seconds(10),
                OrderSide::Sell,
                OrderEventType::Trade,
            ),
            flow(
                now + Duration::seconds(15),
                OrderSide::Sell,
                OrderEventType::Trade,
            ),
        ];

        let (lambda_bid, lambda_ask, mu_bid, mu_ask) = calculate_rates(&events);

        assert!((lambda_bid - 0.1).abs() < 1e-9);
        assert!((lambda_ask - 0.2).abs() < 1e-9);
        assert!((mu_bid - (1.0 / 15.0)).abs() < 1e-9);
        assert!((mu_ask - (2.0 / 15.0)).abs() < 1e-9);
    }

    #[test]
    fn poisson_parameter_update_uses_ema_and_queue_cap() {
        let now = Utc::now();
        let current = PoissonParameters::from_initial_lambda(4.0, now);
        let events = vec![
            flow(now, OrderSide::Buy, OrderEventType::Trade),
            flow(
                now + Duration::seconds(10),
                OrderSide::Sell,
                OrderEventType::Trade,
            ),
        ];

        let next =
            estimate_poisson_parameters(&current, &events, 0.25, now + Duration::seconds(10));

        assert!(next.lambda_bid < current.lambda_bid);
        assert!(next.lambda_ask < current.lambda_ask);
        assert!(next.avg_queue_bid > current.avg_queue_bid);
    }

    #[test]
    fn fill_update_sets_last_trade_time_and_smooths_lambda() {
        let now = Utc::now();
        let current = PoissonParameters::from_initial_lambda(2.0, now);
        let next = update_poisson_params_on_fill(&current, now + Duration::seconds(30));

        assert_eq!(next.last_trade_time, Some(now + Duration::seconds(30)));
        assert!((next.lambda_bid - 2.0).abs() < 1e-9);
    }

    #[test]
    fn market_activity_factor_matches_lambda_thresholds() {
        let now = Utc::now();
        let mut params = PoissonParameters::from_initial_lambda(0.5, now);
        assert_eq!(calculate_market_activity_factor(&params), 1.2);
        params.lambda_bid = 6.0;
        params.lambda_ask = 6.0;
        assert_eq!(calculate_market_activity_factor(&params), 0.9);
        params.lambda_bid = 12.0;
        params.lambda_ask = 12.0;
        assert_eq!(calculate_market_activity_factor(&params), 0.8);
    }

    #[test]
    fn optimal_spread_applies_queue_urgency_inventory_and_max_clamp() {
        let now = Utc::now();
        let config = base_config();
        let mut params = PoissonParameters::from_initial_lambda(0.5, now);
        params.avg_queue_bid = 500.0;
        params.avg_queue_ask = 500.0;
        params.last_trade_time = Some(now - Duration::seconds(120));
        let mut state = MMStrategyState::new(now);
        state.inventory = 0.0;

        let quote = calculate_optimal_spread(&config, &params, &state, 20.0, now);

        assert_eq!(quote.bid_spread, config.trading.max_spread_bp / 10000.0);
        assert_eq!(quote.ask_spread, config.trading.max_spread_bp / 10000.0);
        assert!(quote.urgency_factor < 1.0);
    }

    #[test]
    fn optimal_spread_skews_quotes_for_long_and_short_inventory() {
        let now = Utc::now();
        let config = base_config();
        let params = PoissonParameters::from_initial_lambda(3.0, now);
        let mut long_state = MMStrategyState::new(now);
        long_state.inventory = 10.0;
        let mut short_state = MMStrategyState::new(now);
        short_state.inventory = -10.0;

        let long_quote = calculate_optimal_spread(&config, &params, &long_state, 20.0, now);
        let short_quote = calculate_optimal_spread(&config, &params, &short_state, 20.0, now);

        assert!(long_quote.bid_spread > long_quote.ask_spread);
        assert!(short_quote.bid_spread > short_quote.ask_spread);
    }

    #[test]
    fn precision_helpers_round_exchange_safe_values() {
        let config = base_config();
        let symbol = symbol_info();

        assert_eq!(
            round_price_for_side(
                19.997,
                PriceRoundingSide::Bid,
                &config.trading,
                Some(&symbol)
            ),
            19.99
        );
        assert_eq!(
            round_price_for_side(
                19.991,
                PriceRoundingSide::Ask,
                &config.trading,
                Some(&symbol)
            ),
            20.0
        );
        assert_eq!(round_quantity(1.239, &config.trading, Some(&symbol)), 1.23);
        assert_eq!(round_quantity(-1.0, &config.trading, Some(&symbol)), 0.0);
    }

    #[test]
    fn post_only_adjustment_and_error_classifiers_match_legacy_patterns() {
        let config = base_config();
        let symbol = symbol_info();

        assert_eq!(
            adjust_post_only_price(OrderSide::Buy, 20.0, &config.trading, Some(&symbol)),
            Some(19.99)
        );
        assert_eq!(
            adjust_post_only_price(OrderSide::Sell, 20.0, &config.trading, Some(&symbol)),
            Some(20.01)
        );
        assert!(is_post_only_reject("POST_ONLY_REJECT -5022"));
        assert!(is_order_missing_error("Unknown order sent -2011"));
        assert!(is_reduce_only_rejection(
            "ReduceOnly Order is rejected -2022"
        ));
    }

    #[test]
    fn order_intent_side_counterpart_tag_and_position_side_are_stable() {
        assert_eq!(OrderIntent::OpenLong.side(), OrderSide::Buy);
        assert_eq!(OrderIntent::OpenLong.counterpart(), OrderIntent::CloseLong);
        assert_eq!(OrderIntent::CloseShort.side(), OrderSide::Buy);
        assert_eq!(OrderIntent::OpenShort.tag(), "OS");
        assert_eq!(OrderIntent::CloseLong.position_side(), "LONG");
        assert!(OrderIntent::CloseLong.reduce_only());
    }

    #[test]
    fn state_register_detach_and_trim_maintain_indexes() {
        let now = Utc::now();
        let mut state = MMStrategyState::new(now);
        state.register_order(
            OrderIntent::OpenLong,
            "client-1".to_string(),
            order("exchange-1", None, OrderSide::Buy, now),
        );
        state.register_order(
            OrderIntent::OpenLong,
            "client-2".to_string(),
            order("exchange-2", None, OrderSide::Buy, now),
        );

        assert!(!state.active_buy_orders.contains_key("exchange-1"));
        assert_eq!(state.buy_client_to_exchange["client-2"], "exchange-2");

        let detached = state
            .detach_order_by_client("client-2")
            .expect("order should detach");
        assert_eq!(detached.0, OrderIntent::OpenLong);
        assert!(state.order_slots.is_empty());

        state.order_slots.insert(
            OrderIntent::OpenShort,
            OrderSlotInfo {
                exchange_id: "orphan".to_string(),
                client_id: "orphan-client".to_string(),
            },
        );
        state
            .client_to_slot
            .insert("orphan-client".to_string(), OrderIntent::OpenShort);
        state
            .exchange_to_slot
            .insert("orphan".to_string(), OrderIntent::OpenShort);
        state.trim_slot_orders();
        assert!(state.order_slots.is_empty());
        assert!(state.client_to_slot.is_empty());
        assert!(state.exchange_to_slot.is_empty());
    }

    #[test]
    fn order_plan_builds_open_orders_and_respects_reduce_threshold() {
        let now = Utc::now();
        let config = base_config();
        let symbol = symbol_info();
        let mut state = MMStrategyState::new(now);
        state.short_inventory = 1.0;
        let orderbook = orderbook(now);
        let spreads = SpreadQuote {
            bid_spread: 0.001,
            ask_spread: 0.001,
            inventory_ratio: 0.0,
            activity_factor: 1.0,
            urgency_factor: 1.0,
        };

        let plan = build_order_plan(
            &config,
            &state,
            &orderbook,
            20.0,
            &spreads,
            Some(&symbol),
            true,
        )
        .expect("plan should build");

        assert!(plan.reduce_buy);
        assert!(plan
            .orders
            .iter()
            .any(|order| order.intent == OrderIntent::CloseShort));
        assert!(!plan
            .orders
            .iter()
            .any(|order| order.intent == OrderIntent::OpenShort));
    }

    #[test]
    fn filled_intent_cancels_counterpart_before_replenishment() {
        let now = Utc::now();
        let mut state = MMStrategyState::new(now);
        state.register_order(
            OrderIntent::CloseLong,
            "client-close-long".to_string(),
            order("exchange-close-long", Some(20.1), OrderSide::Sell, now),
        );

        let cancel = filled_intent_cancels_counterpart(&state, OrderIntent::OpenLong)
            .expect("counterpart cancel should build");

        assert_eq!(cancel.intent, OrderIntent::CloseLong);
        assert_eq!(cancel.reason, CancelReason::CounterpartFilled);
    }

    #[test]
    fn risk_snapshot_and_actions_use_net_and_gross_inventory() {
        let now = Utc::now();
        let config = base_config();
        let mut state = MMStrategyState::new(now);
        state.long_inventory = 8.0;
        state.short_inventory = 3.0;
        state.avg_price = 25.0;
        state.daily_pnl = -51.0;

        let snapshot = build_risk_snapshot(&config, &state, 20.0, now);

        assert_eq!(snapshot.net_inventory, 5.0);
        assert_eq!(snapshot.inventory_ratio, Some(0.16));
        assert_eq!(snapshot.unrealized_pnl, -25.0);
        assert_eq!(
            evaluate_risk(&config, &snapshot),
            PoissonRiskAction::HaltDailyLoss { daily_pnl: -51.0 }
        );
    }

    #[test]
    fn market_stream_symbol_maps_stable_quote_aliases() {
        let streams = build_market_streams("binance", "NEAR/USDC");
        assert_eq!(streams[0], "nearusdt@depth20@100ms");
        assert_eq!(streams[1], "nearusdt@trade");
    }

    fn base_config() -> PoissonMMConfig {
        serde_yaml::from_str(CONFIG_YAML).expect("config parses")
    }

    fn symbol_info() -> SymbolInfo {
        SymbolInfo {
            base_asset: "NEAR".to_string(),
            quote_asset: "USDC".to_string(),
            tick_size: 0.01,
            step_size: 0.01,
            min_notional: 5.0,
            price_precision: 2,
            quantity_precision: 2,
        }
    }

    fn orderbook(now: DateTime<Utc>) -> LocalOrderBook {
        LocalOrderBook {
            bids: vec![(19.99, 10.0)],
            asks: vec![(20.01, 10.0)],
            last_update: now,
        }
    }

    fn flow(now: DateTime<Utc>, side: OrderSide, event_type: OrderEventType) -> OrderFlowEvent {
        OrderFlowEvent {
            timestamp: now,
            side,
            price: 20.0,
            quantity: 1.0,
            event_type,
        }
    }

    fn order(
        id: &str,
        price: Option<f64>,
        side: OrderSide,
        timestamp: DateTime<Utc>,
    ) -> PoissonOrderRecord {
        PoissonOrderRecord {
            id: id.to_string(),
            client_order_id: None,
            side,
            price,
            quantity: 1.0,
            status: PoissonOrderStatus::Open,
            timestamp,
        }
    }

    fn assert_secret_free(value: &Value) {
        let serialized = value.to_string().to_ascii_lowercase();
        for forbidden in ["api_key", "secret", "password", "private_key"] {
            assert!(
                !serialized.contains(forbidden),
                "{forbidden} should not appear in serialized output"
            );
        }
    }

    const CONFIG_YAML: &str = r#"
name: poisson-market-maker
enabled: true
version: "1"
account:
  account_id: account-1
  exchange: venue-a
trading:
  symbol: NEAR/USDC
  order_size_usdc: 20.0
  max_inventory: 1000.0
  min_spread_bp: 10.0
  max_spread_bp: 150.0
  refresh_interval_secs: 5
  price_precision: 2
  quantity_precision: 2
poisson:
  observation_window_secs: 60
  min_samples: 10
  smoothing_alpha: 0.2
  depth_levels: 20
  confidence_interval: 0.95
  initial_lambda: 3.0
risk:
  max_unrealized_loss: 100.0
  max_daily_loss: 50.0
  inventory_skew_limit: 0.5
  stop_loss_pct: 0.05
"#;
}
