use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, ExecutionCancelCommand, ExecutionIntent, ExecutionOrderCommand,
    HealthSeverity, MarketDataChannel, MarketDataSubscription, MarketType, OrderSide, OrderType,
    RequiredAccountPermission, RiskCapability, RiskCapabilityDeclaration, StrategyCommandSchema,
    StrategyConfigSchema, StrategyContext, StrategyEvent, StrategyHealthIssue, StrategyInstanceId,
    StrategyRuntime, StrategySnapshot, StrategySnapshotSchema, StrategySpec, StrategyStatus,
    TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod app_runtime;
pub mod core;
pub mod gateway_scan;
pub mod live_plan;
pub mod runtime_contract;

pub use app_runtime::{
    FundingAppRuntime, FundingExecutionIntentSummary, FundingNotification, FundingRuntimeCycle,
    FundingStorageEvent,
};
pub use core::{
    build_startup_markdown, candidate_from_snapshot, candidate_orderable, planned_quantity,
    select_exchange_funding, ExchangeFundingSelection, FundingCandidate, FundingCoreConfig,
    FundingInstrument, FundingScanReport, FundingSnapshot, FundingSymbol,
};
pub use gateway_scan::{
    build_report_from_gateway_scan_bundles, funding_snapshot_from_gateway,
    instrument_from_symbol_rules, GatewayFundingScanBundle,
};
pub use live_plan::{
    build_live_plan, build_live_plan_at, build_live_plan_markdown, build_live_result_markdown,
    funding_client_order_id, next_scan_time, plan_entry_from_selection, ActionAckSummary,
    CancelAckSummary, FillSummary, FundingLiveExchangePlan, FundingLiveExchangeResult,
    FundingLiveExchangeSkip, FundingLivePlan, FundingLiveStatus,
};
pub use runtime_contract::{
    build_runtime_contract, default_runtime_contract, FundingDashboardSnapshot,
    FundingDashboardSnapshotProvider, FundingExecutionProvider, FundingMarketDataProvider,
    FundingNotificationProvider, FundingRuntimeContract, FundingRuntimeMode,
    FundingStorageProvider, RuntimeProviderContract, RuntimeTaskContract,
};

pub const STRATEGY_KIND: &str = "funding_arbitrage";
pub const DISPLAY_NAME: &str = "Funding Arbitrage";
pub const MIGRATED_FROM: &str = "legacy-strategy:funding_rate_arbitrage";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingArbitrageStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for FundingArbitrageStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingArbitrageConfig {
    #[serde(default = "default_funding_venues")]
    pub venues: Vec<String>,
    #[serde(default = "default_funding_symbols")]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub min_funding_edge_bps: f64,
    #[serde(default = "default_funding_notional")]
    pub max_position_notional_quote: String,
    #[serde(default)]
    pub dry_run: bool,
}

impl Default for FundingArbitrageConfig {
    fn default() -> Self {
        Self {
            venues: vec!["bitget".to_string()],
            symbols: vec!["BTC/USDT".to_string()],
            min_funding_edge_bps: 0.0,
            max_position_notional_quote: "20".to_string(),
            dry_run: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingArbitrageSnapshotPayload {
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
    pub configured_venues: Vec<String>,
    pub configured_symbols: Vec<String>,
    pub market_data_subscriptions: Vec<MarketDataSubscription>,
}

#[derive(Debug, Clone)]
pub struct FundingArbitrageRuntime {
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
    config: Option<FundingArbitrageConfig>,
    market_data_subscriptions: Vec<MarketDataSubscription>,
}

impl FundingArbitrageRuntime {
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
        }
    }

    fn snapshot_payload(&self) -> FundingArbitrageSnapshotPayload {
        FundingArbitrageSnapshotPayload {
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
            configured_venues: self
                .config
                .as_ref()
                .map(|config| config.venues.clone())
                .unwrap_or_default(),
            configured_symbols: self
                .config
                .as_ref()
                .map(|config| config.symbols.clone())
                .unwrap_or_default(),
            market_data_subscriptions: self.market_data_subscriptions.clone(),
        }
    }
}

impl Default for FundingArbitrageRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for FundingArbitrageRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        let config: FundingArbitrageConfig = serde_json::from_value(ctx.config().clone())?;
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.last_event_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.market_data_subscriptions = funding_market_data_subscriptions(&config);
        self.config = Some(config);
        self.handled_events = 0;
        self.market_data_events = 0;
        self.execution_events = 0;
        self.account_events = 0;
        self.operator_commands = 0;
        self.timer_events = 0;
        self.last_market_data_at = None;
        self.last_execution_at = None;
        self.last_account_sync_at = None;
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.status = StrategyStatus::Stopped;
        Ok(())
    }

    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()> {
        self.handled_events += 1;
        self.last_event_at = Some(event_timestamp(&event));
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
            StrategyEvent::Timer(_) => self.timer_events += 1,
        }
        Ok(())
    }

    async fn snapshot(&self) -> anyhow::Result<StrategySnapshot> {
        Ok(StrategySnapshot {
            schema_version: 1,
            instance_id: self.instance_id.clone(),
            strategy_kind: STRATEGY_KIND.to_string(),
            strategy_id: self.strategy_id.clone(),
            run_id: self.run_id.clone(),
            captured_at: Utc::now(),
            status: self.status.clone(),
            payload: serde_json::to_value(self.snapshot_payload())?,
            health: runtime_health_issues(
                Utc::now(),
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
            "Funding arbitrage strategy runtime contract with adapter-free selection and live plan core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places spot/perpetual funding arbitrage orders",
            ),
            risk_capability(RiskCapability::CancelOrders, "Cancels stale funding orders"),
            risk_capability(RiskCapability::Hedging, "Maintains hedged funding exposure"),
            risk_capability(
                RiskCapability::CrossAccountRead,
                "Reads balances, positions, and funding state across venues",
            ),
        ],
        market_data_subscriptions: funding_market_data_subscriptions(&FundingArbitrageConfig::default()),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadPositions,
            AccountPermission::ReadOrders,
            AccountPermission::TradeSpot,
            AccountPermission::TradePerpetual,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            ("legacy_module".to_string(), json!(MIGRATED_FROM)),
            ("partial_core_migration".to_string(), json!(false)),
            ("runtime_contract_migration".to_string(), json!(true)),
            (
                "migrated_core_modules".to_string(),
                json!([
                    "core_config",
                    "scanner_selection",
                    "startup_markdown",
                    "live_plan",
                    "live_result_markdown",
                    "schedule_time",
                    "app_runtime_cycle",
                    "runtime_contract",
                    "task_orchestration_contract",
                    "market_data_provider_contract",
                    "execution_provider_contract",
                    "storage_provider_contract",
                    "dashboard_snapshot_contract",
                    "notification_provider_contract"
                ]),
            ),
            ("remaining_legacy_modules".to_string(), json!([])),
            (
                "market_data_channels".to_string(),
                json!([MarketDataChannel::FundingRate]),
            ),
            (
                "primary_market_type".to_string(),
                json!(MarketType::Perpetual),
            ),
        ]),
    }
}

pub fn funding_market_data_subscriptions(
    config: &FundingArbitrageConfig,
) -> Vec<MarketDataSubscription> {
    let channels = vec![
        MarketDataChannel::FundingRate,
        MarketDataChannel::MarkPrice,
        MarketDataChannel::IndexPrice,
    ];

    config
        .venues
        .iter()
        .filter(|venue| !venue.trim().is_empty())
        .flat_map(|venue| {
            let exchange_id = venue.trim().to_string();
            let channels = channels.clone();
            config
                .symbols
                .iter()
                .filter(|symbol| !symbol.trim().is_empty())
                .map(move |symbol| MarketDataSubscription {
                    exchange_id: exchange_id.clone(),
                    symbol: symbol.trim().to_string(),
                    market_type: MarketType::Perpetual,
                    channels: channels.clone(),
                })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn funding_live_entry_to_execution_command(
    ctx: &StrategyContext,
    entry: &FundingLiveExchangePlan,
    risk_profile_id: &str,
    side: OrderSide,
    quantity: f64,
    client_order_id: String,
    requested_at: DateTime<Utc>,
) -> ExecutionOrderCommand {
    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: client_order_id.clone(),
        idempotency_key: execution_idempotency_key(ctx, &client_order_id),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: entry.exchange.clone(),
        symbol: entry.exchange_symbol.clone(),
        side,
        order_type: OrderType::Market,
        quantity: quantity.to_string(),
        price: None,
        time_in_force: Some(TimeInForce::ImmediateOrCancel),
        reduce_only: false,
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            (
                "source_plan".to_string(),
                json!("funding_live_exchange_plan"),
            ),
            (
                "canonical_symbol".to_string(),
                json!(entry.canonical_symbol.as_pair()),
            ),
            (
                "funding_rate_pct".to_string(),
                json!(entry.funding_rate_pct),
            ),
        ]),
    }
}

pub fn funding_cancel_command(
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

pub fn funding_execution_intent(
    ctx: &StrategyContext,
    intent_id: &str,
    requested_at: DateTime<Utc>,
    payload: Value,
) -> ExecutionIntent {
    ExecutionIntent {
        schema_version: 1,
        intent_kind: "funding_arbitrage_execution_plan".to_string(),
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        idempotency_key: execution_idempotency_key(ctx, intent_id),
        requested_at,
        payload,
    }
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": [
                "venues",
                "symbols",
                "min_funding_edge_bps",
                "max_position_notional_quote"
            ],
            "properties": {
                "venues": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "symbols": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "min_funding_edge_bps": { "type": "number", "minimum": 0.0 },
                "max_position_notional_quote": {
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
            "configured_venues": {
                "type": "array",
                "items": { "type": "string" }
            },
            "configured_symbols": {
                "type": "array",
                "items": { "type": "string" }
            },
            "market_data_subscriptions": { "type": "array" }
        }
    })
}

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    ["pause", "resume", "stop", "refresh_funding_scan"]
        .into_iter()
        .map(|command_kind| StrategyCommandSchema {
            command_kind: command_kind.to_string(),
            description: Some(format!("Funding arbitrage runtime {command_kind} command")),
            payload_schema: json!({
                "type": "object",
                "additionalProperties": true
            }),
        })
        .collect()
}

fn runtime_health_issues(
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
        "funding_data_stale",
        "No recent funding market data event observed",
        900,
    );
    push_staleness_issue(
        &mut issues,
        now,
        started_at,
        last_account_sync_at,
        "account_sync_stale",
        "No recent account sync event observed",
        1800,
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

fn execution_idempotency_key(ctx: &StrategyContext, client_order_id: &str) -> String {
    format!("{}:{}:{}", ctx.strategy_id(), ctx.run_id(), client_order_id)
}

fn default_funding_venues() -> Vec<String> {
    vec!["bitget".to_string()]
}

fn default_funding_symbols() -> Vec<String> {
    vec!["BTC/USDT".to_string()]
}

fn default_funding_notional() -> String {
    "20".to_string()
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
    use rustcta_strategy_sdk::{
        ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
        ExecutionOrderAck, ExecutionOrderCommand, SdkResult, StrategyExecutionClient,
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
        assert_eq!(spec.snapshot_schema.schema_version, 1);
        assert_eq!(spec.snapshot_schema.json_schema["type"], json!("object"));
        assert!(spec.snapshot_schema.json_schema["properties"]["handled_events"].is_object());
        assert_eq!(
            spec.supported_commands
                .iter()
                .map(|command| command.command_kind.as_str())
                .collect::<Vec<_>>(),
            vec!["pause", "resume", "stop", "refresh_funding_scan"]
        );
        assert!(!spec.market_data_subscriptions.is_empty());
        assert_eq!(
            spec.market_data_subscriptions[0].channels,
            vec![
                MarketDataChannel::FundingRate,
                MarketDataChannel::MarkPrice,
                MarketDataChannel::IndexPrice
            ]
        );
        assert_eq!(spec.metadata["runtime_contract_migration"], json!(true));
        assert_eq!(spec.metadata["remaining_legacy_modules"], json!([]));
        assert_secret_free(&serde_json::to_value(spec).expect("spec should serialize"));
    }

    #[test]
    fn config_parse_and_market_data_subscription_should_use_sdk_contract() {
        let config: FundingArbitrageConfig = serde_json::from_value(json!({
            "venues": ["bitget", "gate"],
            "symbols": ["BTC/USDT"],
            "min_funding_edge_bps": 2.0,
            "max_position_notional_quote": "20",
            "dry_run": true
        }))
        .expect("strategy config should parse");
        let core_config: FundingCoreConfig = serde_json::from_value(json!({
            "mode": "observe",
            "universe": {
                "enabled_exchanges": ["bitget"],
                "symbol_allowlist": ["BTC/USDT"]
            }
        }))
        .expect("core config should parse");

        let subscriptions = funding_market_data_subscriptions(&config);

        assert!(core_config.validate().is_ok());
        assert_eq!(subscriptions.len(), 2);
        assert_eq!(subscriptions[0].exchange_id, "bitget");
        assert_eq!(subscriptions[0].symbol, "BTC/USDT");
        assert_eq!(subscriptions[0].market_type, MarketType::Perpetual);
        assert_eq!(
            subscriptions[0].channels,
            vec![
                MarketDataChannel::FundingRate,
                MarketDataChannel::MarkPrice,
                MarketDataChannel::IndexPrice
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_should_be_secret_free() {
        let mut runtime = FundingArbitrageRuntime::new();
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
                "symbols": ["BTC/USDT"]
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

    #[test]
    fn dry_run_execution_intent_should_be_adapter_free_and_idempotent() {
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            serde_json::to_value(FundingArbitrageConfig::default())
                .expect("config should serialize"),
            execution,
        );
        let intent = funding_execution_intent(
            &ctx,
            "funding-plan-1",
            Utc::now(),
            json!({
                "dry_run": true,
                "exchange": "bitget",
                "symbol": "BTC/USDT",
                "notional_quote": "20"
            }),
        );

        assert_eq!(intent.intent_kind, "funding_arbitrage_execution_plan");
        assert_eq!(intent.tenant_id, "tenant-1");
        assert_eq!(intent.account_id, "account-1");
        assert_eq!(intent.idempotency_key, "strategy-1:run-1:funding-plan-1");
        assert_eq!(intent.payload["dry_run"], json!(true));
        assert_secret_free(&serde_json::to_value(intent).expect("intent should serialize"));
    }

    #[test]
    fn manifest_should_keep_core_library_adapter_free() {
        let manifest = include_str!("../Cargo.toml");
        assert!(manifest.contains("rustcta-strategy-sdk.workspace = true"));
        assert!(manifest.contains("rustcta-exchange-api.workspace = true"));
        assert!(manifest.contains("rustcta-exchange-gateway.workspace = true"));
        for forbidden in ["legacy exchange adapter path", "okx", "bitget"] {
            assert!(
                !manifest.contains(forbidden),
                "{forbidden} should not appear in manifest"
            );
        }
        let library_source = include_str!("lib.rs");
        let gateway_crate_name = concat!("rustcta", "_exchange", "_gateway");
        assert!(
            !library_source.contains(gateway_crate_name),
            "core strategy library should stay adapter-free; the runtime binary owns gateway wiring"
        );
    }

    #[test]
    fn runtime_contract_should_be_adapter_free_and_order_safe() {
        let config = FundingCoreConfig {
            mode: "live".to_string(),
            selection: core::SelectionConfig {
                max_seconds_to_settlement_at_scan: Some(120),
                ..core::SelectionConfig::default()
            },
            ..FundingCoreConfig::default()
        };
        let contract = build_runtime_contract(&config, Utc::now());

        assert_eq!(contract.strategy_kind, STRATEGY_KIND);
        assert_eq!(contract.mode, FundingRuntimeMode::LiveRequested);
        assert!(!contract.live_orders_enabled_by_default);
        assert!(!contract.dashboard_snapshot.live_orders_enabled);
        assert!(contract.market_data_provider.adapter_free);
        assert!(!contract.market_data_provider.concrete_adapter_dependency);
        assert!(contract.execution_provider.adapter_free);
        assert!(!contract.execution_provider.concrete_adapter_dependency);
        assert!(contract
            .tasks
            .iter()
            .any(|task| task.task_kind == "publish_dashboard_snapshot"));
        assert_secret_free(&serde_json::to_value(contract).expect("contract should serialize"));
    }

    #[test]
    fn app_runtime_cycle_should_plan_store_snapshot_and_notify_without_orders() {
        let config = FundingCoreConfig::default();
        let now = Utc::now();
        let report = FundingScanReport {
            generated_at: now,
            threshold: config.selection.min_funding_rate,
            threshold_pct: config.selection.min_funding_rate * 100.0,
            selections: vec![ExchangeFundingSelection {
                exchange: "bitget".to_string(),
                selected: Some(FundingCandidate {
                    exchange: "bitget".to_string(),
                    canonical_symbol: FundingSymbol::new("btc", "usdt"),
                    exchange_symbol: Some("BTCUSDT".to_string()),
                    funding_rate: config.selection.min_funding_rate,
                    funding_rate_pct: config.selection.min_funding_rate * 100.0,
                    predicted_funding_rate: None,
                    mark_price: Some(100.0),
                    index_price: None,
                    next_funding_time: Some(now + chrono::Duration::minutes(10)),
                    seconds_to_settlement: Some(600),
                    snapshot_age_ms: 0,
                    qualifies: true,
                }),
                scanned_symbols: 1,
                funding_snapshots: 1,
                eligible_candidates: 1,
                skipped_reason: None,
            }],
            errors: Vec::new(),
        };
        let mut runtime = FundingAppRuntime::new(config);
        let cycle = runtime.run_cycle(&report, now);

        assert_eq!(cycle.live_plan.entries.len(), 1);
        assert_eq!(cycle.dashboard_snapshot.planned_entries, 1);
        assert!(!cycle.execution.live_orders_enabled);
        assert_eq!(cycle.execution.submitted_intents, 0);
        assert!(cycle
            .storage_events
            .iter()
            .any(|event| event.event_kind == "funding_live_plan_entries"));
        assert_eq!(cycle.notifications.len(), 1);
        assert_secret_free(&serde_json::to_value(cycle).expect("cycle should serialize"));
    }

    #[test]
    fn migrated_core_should_validate_observe_and_live_modes() {
        let mut observe = FundingCoreConfig::default();
        observe.execution.notional_usdt = 50.0;
        assert!(
            observe.validate().is_ok(),
            "observe mode should not validate live execution sizing"
        );

        let mut live = FundingCoreConfig {
            mode: "live".to_string(),
            ..FundingCoreConfig::default()
        };
        assert!(live.validate().is_err());
        live.selection.max_seconds_to_settlement_at_scan = Some(120);
        assert!(live.validate().is_ok());
        live.execution.notional_usdt = 20.01;
        assert!(live.validate().is_err());
    }

    #[test]
    fn migrated_core_should_select_threshold_equal_candidate() {
        let mut config = FundingCoreConfig::default();
        config.selection.max_seconds_to_settlement_at_scan = Some(600);
        let now = Utc::now();
        let selection = select_exchange_funding(
            "bitget",
            [FundingSnapshot {
                exchange: "bitget".to_string(),
                canonical_symbol: FundingSymbol::new("btc", "usdt"),
                exchange_symbol: Some("BTCUSDT".to_string()),
                funding_rate: config.selection.min_funding_rate,
                predicted_funding_rate: None,
                mark_price: Some(100.0),
                index_price: None,
                next_funding_time: Some(now + chrono::Duration::seconds(600)),
                recv_ts: now,
            }],
            &[funding_instrument_for_test()],
            &config,
            now,
        );

        let selected = selection.selected.expect("threshold equal should qualify");
        assert!(selected.qualifies);
        assert_eq!(selected.funding_rate_pct, -0.5);
        assert_eq!(selected.seconds_to_settlement, Some(600));
    }

    #[test]
    fn migrated_core_should_apply_snapshot_age_and_symbol_filters() {
        let mut config = FundingCoreConfig::default();
        config.universe.symbol_blocklist = vec!["BTC/USDT".to_string()];
        let now = Utc::now();
        assert!(!core::symbol_allowed(
            &FundingSymbol::new("btc", "usdt"),
            &config
        ));
        assert!(core::symbol_allowed(
            &FundingSymbol::new("eth", "usdt"),
            &config
        ));
        assert!(!core::symbol_allowed(
            &FundingSymbol::new("草根文化", "usdt"),
            &config
        ));

        config.universe.symbol_blocklist.clear();
        let fresh_at_limit = FundingSnapshot {
            exchange: "gate".to_string(),
            canonical_symbol: FundingSymbol::new("eth", "usdt"),
            exchange_symbol: Some("ETH_USDT".to_string()),
            funding_rate: -0.01,
            predicted_funding_rate: None,
            mark_price: Some(100.0),
            index_price: None,
            next_funding_time: Some(now + chrono::Duration::minutes(5)),
            recv_ts: now
                - chrono::Duration::milliseconds(config.selection.max_funding_snapshot_age_ms),
        };
        assert!(candidate_from_snapshot(fresh_at_limit, &config, now).is_some());

        let stale = FundingSnapshot {
            recv_ts: now
                - chrono::Duration::milliseconds(config.selection.max_funding_snapshot_age_ms + 1),
            exchange_symbol: Some("ETH_USDT".to_string()),
            ..funding_snapshot_for_test(now)
        };
        assert!(candidate_from_snapshot(stale, &config, now).is_none());
    }

    #[test]
    fn migrated_core_should_preserve_orderable_wide_defaults() {
        let mut config = FundingCoreConfig::default();
        config.mode = "observe".to_string();
        let now = Utc::now();
        let mut candidate =
            candidate_from_snapshot(funding_snapshot_for_test(now), &config, now).unwrap();

        assert!(candidate_orderable(&candidate, &[], &config));
        candidate.mark_price = None;
        candidate.index_price = None;
        assert!(candidate_orderable(
            &candidate,
            &[funding_instrument_for_test()],
            &config
        ));

        candidate.mark_price = Some(10.0);
        let mut restrictive = funding_instrument_for_test();
        restrictive.min_qty = 100.0;
        assert!(!candidate_orderable(&candidate, &[restrictive], &config));
    }

    #[test]
    fn migrated_core_should_reject_unorderable_live_candidates() {
        let mut config = FundingCoreConfig::default();
        config.mode = "live".to_string();
        config.execution.notional_usdt = 10.0;
        let now = Utc::now();
        let mut candidate =
            candidate_from_snapshot(funding_snapshot_for_test(now), &config, now).unwrap();

        assert!(!candidate_orderable(&candidate, &[], &config));

        candidate.mark_price = None;
        candidate.index_price = None;
        assert!(!candidate_orderable(
            &candidate,
            &[funding_instrument_for_test()],
            &config
        ));

        candidate.mark_price = Some(100.0);
        let mut restrictive = funding_instrument_for_test();
        restrictive.min_qty = 1.0;
        assert_eq!(planned_quantity(10.0, 100.0, &restrictive), None);
        assert!(!candidate_orderable(&candidate, &[restrictive], &config));
    }

    #[test]
    fn migrated_core_should_render_startup_markdown() {
        let now = Utc::now();
        let report = FundingScanReport {
            generated_at: now,
            threshold: -0.005,
            threshold_pct: -0.5,
            selections: vec![
                ExchangeFundingSelection {
                    exchange: "bitget".to_string(),
                    selected: Some(
                        candidate_from_snapshot(
                            funding_snapshot_for_test(now),
                            &FundingCoreConfig::default(),
                            now,
                        )
                        .expect("candidate"),
                    ),
                    scanned_symbols: 1,
                    funding_snapshots: 1,
                    eligible_candidates: 1,
                    skipped_reason: None,
                },
                ExchangeFundingSelection {
                    exchange: "gate".to_string(),
                    selected: None,
                    scanned_symbols: 2,
                    funding_snapshots: 0,
                    eligible_candidates: 0,
                    skipped_reason: Some("no funding snapshots loaded".to_string()),
                },
            ],
            errors: vec![core::ExchangeScanError {
                exchange: "binance".to_string(),
                stage: "load_funding",
                message: "timeout".to_string(),
            }],
        };

        let markdown = build_startup_markdown(&report);

        assert!(markdown.contains("no orders"));
        assert!(markdown.contains("BTC/USDT"));
        assert!(markdown.contains("-0.6000%"));
        assert!(markdown.contains("no funding snapshots loaded"));
        assert!(markdown.contains("timeout"));
        assert!(markdown.contains("does not submit any orders"));
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

    fn funding_snapshot_for_test(now: chrono::DateTime<Utc>) -> FundingSnapshot {
        FundingSnapshot {
            exchange: "bitget".to_string(),
            canonical_symbol: FundingSymbol::new("btc", "usdt"),
            exchange_symbol: Some("BTCUSDT".to_string()),
            funding_rate: -0.006,
            predicted_funding_rate: None,
            mark_price: Some(100.0),
            index_price: None,
            next_funding_time: Some(now + chrono::Duration::minutes(5)),
            recv_ts: now,
        }
    }

    fn funding_instrument_for_test() -> FundingInstrument {
        FundingInstrument {
            exchange: "bitget".to_string(),
            canonical_symbol: FundingSymbol::new("btc", "usdt"),
            exchange_symbol: "BTCUSDT".to_string(),
            contract_type: "linear_perpetual".to_string(),
            status: "trading".to_string(),
            contract_size: 1.0,
            quantity_step: 0.001,
            min_qty: 0.001,
            min_notional: 5.0,
        }
    }
}
