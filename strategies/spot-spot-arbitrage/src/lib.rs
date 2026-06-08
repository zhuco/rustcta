use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, MarketDataSubscription as SdkMarketDataSubscription,
    RequiredAccountPermission, RiskCapability, RiskCapabilityDeclaration, StrategyCommandSchema,
    StrategyConfigSchema, StrategyContext, StrategyEvent, StrategyInstanceId, StrategyRuntime,
    StrategySnapshot, StrategySnapshotSchema, StrategySpec, StrategyStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod config;
pub mod core;
pub mod execution;
pub mod fees;
pub mod inventory;
pub mod lifecycle;
pub mod market_data;
pub mod paper;
pub mod replay_report;
pub mod runtime_contract;

pub use config::{
    InventoryRebalanceConfig, KillSwitchConfig, KuCoinSpotRuntimeConfig, LiveDryRunConfig,
    LivePreflightConfig, MarketDataMode, MarketType, OrderReconciliationConfig, RecorderConfig,
    ReplayConfig, RestPollingMarketDataConfig, SmallLiveGateConfig, SpotSpotTakerArbitrageConfig,
    VenueCostConfig, VenueFeeOverride, VenueRuntimeConfig, VenueSelectionConfig,
    VenueSelectionEstimate, WebsocketMarketDataConfig,
};
pub use core::{
    calculate_spread, configured_spot_pair, depth_notional,
    spot_rejection_counts_toward_consecutive, BookSource, CachedBook, DirectedVenuePair,
    EventDrivenSpreadEngine, EventDrivenSpreadEngineConfig, EventDrivenSpreadResult,
    OpportunityRecord, OpportunitySummaryRecord, RejectionReason, SimulatedTradeRecord,
    SpotBookEvent, SpotBookEventKind, SpotFeeSource, SpotOrderBookLevel, SpotRiskLimits,
    SpotRiskState, SpotVenue, SpreadEstimate, SummaryReport, TradePnlCategory, TradeSummaryRecord,
};
pub use execution::{
    build_dual_taker_arbitrage_live_order_plan, build_validated_live_order_command,
    spot_live_execution_exchange_allowed, spot_live_execution_symbol_allowed,
    spot_live_execution_symbols, validate_live_order_safety, LiveOrderSafetyDecision,
    SpotExecutionMode, SpotLiveOrderPlan, SpotPairedLiveOrderPlan,
};
pub use fees::{
    fee_model_from_strategy_config, EstimatedPairFees, SpotFeeLookup, SpotFeeModel,
    SpotFeeOverride, SpotFeeOverrides, SpotFeeRate,
};
pub use inventory::{parse_spot_venue, InventoryReservation, PaperAssetState, PaperInventory};
pub use lifecycle::{
    allocation_weights, build_entry_allocations, configured_spot_venues, duration_from_seconds,
    normalize_symbol, ArbitragePairRuntime, ArbitragePairStatus, EntryAllocation, OneSidedExposure,
    OneSidedExposureKind, SpreadDurationTracker,
};
pub use market_data::{market_data_subscriptions, replay_input_path};
pub use paper::{
    consume_levels, parse_paper_exchange, simulate_taker_buy, simulate_taker_pair,
    simulate_taker_sell, SimulatedLeg, SimulatedPairExecution,
};
pub use replay_report::{
    OpportunityDuration, OpportunityDurationTracker, ReplayReport, ReplayReportBuilder,
};
pub use runtime_contract::{
    SpotBookRuntimeSnapshot, SpotFeeSnapshot, SpotInventoryBalanceSnapshot, SpotInventorySnapshot,
    SpotOpportunityPairSnapshot, SpotOpportunityRuntimeSnapshot, SpotOrderPlanSnapshot,
    SpotRuntimeContractSnapshot, SpotRuntimeContractState, SpotRuntimeMarketDataSnapshot,
    SpotRuntimeReadinessSnapshot,
};

pub const STRATEGY_KIND: &str = "spot_spot_arbitrage";
pub const DISPLAY_NAME: &str = "Spot Spot Arbitrage";
pub const MIGRATED_FROM: &str = "legacy-strategy:spot_spot_taker_arbitrage";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpotSpotArbitrageStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for SpotSpotArbitrageStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotSpotArbitrageConfig {
    pub exchanges: Vec<String>,
    pub symbols: Vec<String>,
    pub min_edge_bps: f64,
    pub max_notional_quote: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotSpotArbitrageSnapshotPayload {
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
    pub configured_exchanges: Vec<String>,
    pub configured_symbols: Vec<String>,
    pub trading_mode: Option<String>,
    pub dry_run: Option<bool>,
    pub live_trading_enabled: Option<bool>,
    pub market_data_mode: Option<String>,
    pub market_data_subscriptions: Vec<SdkMarketDataSubscription>,
    pub runtime_contract: SpotRuntimeContractSnapshot,
}

#[derive(Debug, Clone)]
pub struct SpotSpotArbitrageRuntime {
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
    config: Option<SpotSpotTakerArbitrageConfig>,
    market_data_subscriptions: Vec<SdkMarketDataSubscription>,
    runtime_contract: SpotRuntimeContractState,
}

impl SpotSpotArbitrageRuntime {
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
            runtime_contract: SpotRuntimeContractState::default(),
        }
    }

    fn snapshot_payload(&self) -> SpotSpotArbitrageSnapshotPayload {
        SpotSpotArbitrageSnapshotPayload {
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
            configured_exchanges: self
                .config
                .as_ref()
                .map(|config| config.exchanges.clone())
                .unwrap_or_default(),
            configured_symbols: self
                .config
                .as_ref()
                .map(|config| config.symbols.clone())
                .unwrap_or_default(),
            trading_mode: self
                .config
                .as_ref()
                .map(|config| config.trading_mode.clone()),
            dry_run: self.config.as_ref().map(|config| config.dry_run),
            live_trading_enabled: self
                .config
                .as_ref()
                .map(|config| config.live_trading_enabled),
            market_data_mode: self
                .config
                .as_ref()
                .map(|config| format!("{:?}", config.market_data_mode)),
            market_data_subscriptions: self.market_data_subscriptions.clone(),
            runtime_contract: self.runtime_contract.snapshot(),
        }
    }
}

impl Default for SpotSpotArbitrageRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for SpotSpotArbitrageRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        let config: SpotSpotTakerArbitrageConfig = serde_json::from_value(ctx.config().clone())?;
        config.validate_safe_mode()?;
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.last_event_at = Some(ctx.started_at());
        self.handled_events = 0;
        self.market_data_events = 0;
        self.execution_events = 0;
        self.account_events = 0;
        self.operator_commands = 0;
        self.timer_events = 0;
        self.last_market_data_at = None;
        self.last_execution_at = None;
        self.last_account_sync_at = None;
        self.market_data_subscriptions = market_data_subscriptions(&config);
        self.runtime_contract.start(&config);
        self.config = Some(config);
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
                self.runtime_contract.apply_execution(event);
            }
            StrategyEvent::MarketData(event) => {
                self.market_data_events += 1;
                self.last_market_data_at = Some(event.received_at);
                self.runtime_contract.apply_market_data(event);
            }
            StrategyEvent::Account(event) => {
                self.account_events += 1;
                self.last_account_sync_at = Some(event.received_at);
                self.runtime_contract.apply_account(event);
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
            health: Vec::new(),
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
            "Partially migrated spot-spot arbitrage strategy with adapter-free spread/report core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
        risk_capabilities: vec![
            risk_capability(RiskCapability::PlaceOrders, "Places paired spot orders"),
            risk_capability(RiskCapability::CancelOrders, "Cancels stale paired orders"),
            risk_capability(
                RiskCapability::CrossAccountRead,
                "Reads balances and orders across configured venues",
            ),
        ],
        market_data_subscriptions: Vec::new(),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadOrders,
            AccountPermission::TradeSpot,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            ("legacy_module".to_string(), json!(MIGRATED_FROM)),
            ("partial_core_migration".to_string(), json!(true)),
            (
                "migrated_core_modules".to_string(),
                json!([
                    "core_types",
                    "config_loading",
                    "spread_engine",
                    "summary_report",
                    "book_helpers",
                    "risk_state",
                    "paper_inventory",
                    "paper_execution_legs",
                    "lifecycle_state",
                    "replay_report",
                    "fee_integration",
                    "live_safety_gating",
                    "market_data_subscription_plan"
                ]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!([
                    "risk_integration",
                    "exchange_execution_adapter",
                    "websocket_runtime"
                ]),
            ),
        ]),
    }
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": true,
            "required": [
                "exchanges",
                "symbols",
                "max_notional_per_trade",
                "min_notional_per_trade",
                "max_notional_per_symbol",
                "max_total_notional",
                "min_net_spread_bps",
                "min_depth_notional"
            ],
            "properties": {
                "enabled": { "type": "boolean", "default": true },
                "trading_mode": {
                    "type": "string",
                    "enum": ["paper", "live_dry_run", "live"],
                    "default": "paper"
                },
                "exchanges": {
                    "type": "array",
                    "minItems": 2,
                    "items": { "type": "string", "minLength": 1 }
                },
                "symbols": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "quote_asset": { "type": "string", "default": "USDT" },
                "max_notional_per_trade": { "type": "number", "exclusiveMinimum": 0.0 },
                "min_notional_per_trade": { "type": "number", "exclusiveMinimum": 0.0 },
                "max_notional_per_symbol": { "type": "number", "exclusiveMinimum": 0.0 },
                "max_total_notional": { "type": "number", "exclusiveMinimum": 0.0 },
                "min_net_spread_bps": { "type": "number" },
                "min_depth_notional": { "type": "number", "exclusiveMinimum": 0.0 },
                "dry_run": { "type": "boolean", "default": true },
                "live_trading_enabled": { "type": "boolean", "default": false },
                "market_data_mode": {
                    "type": "string",
                    "enum": ["rest_polling", "websocket_cache", "replay"],
                    "default": "rest_polling"
                },
                "websocket": { "type": "object" },
                "rest_polling": { "type": "object" },
                "replay": { "type": "object" },
                "live_preflight": { "type": "object" },
                "live_dry_run": { "type": "object" },
                "order_reconciliation": { "type": "object" },
                "kill_switch": { "type": "object" },
                "small_live_gate": { "type": "object" },
                "inventory_rebalance": { "type": "object" },
                "venue_selection": { "type": "object" }
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
            "configured_exchanges": {
                "type": "array",
                "items": { "type": "string" }
            },
            "configured_symbols": {
                "type": "array",
                "items": { "type": "string" }
            },
            "trading_mode": { "type": ["string", "null"] },
            "dry_run": { "type": ["boolean", "null"] },
            "live_trading_enabled": { "type": ["boolean", "null"] },
            "market_data_mode": { "type": ["string", "null"] },
            "market_data_subscriptions": { "type": "array" },
            "runtime_contract": {
                "type": "object",
                "additionalProperties": true,
                "required": ["readiness", "market_data", "inventory", "fees", "order_plans"],
                "properties": {
                    "readiness": { "type": "object" },
                    "market_data": { "type": "object" },
                    "opportunity": { "type": ["object", "null"] },
                    "order_plans": { "type": "array" },
                    "inventory": { "type": "object" },
                    "fees": { "type": "object" }
                }
            }
        }
    })
}

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    ["pause", "resume", "stop", "rebalance", "reload_config"]
        .into_iter()
        .map(|command_kind| StrategyCommandSchema {
            command_kind: command_kind.to_string(),
            description: Some(format!(
                "Spot-spot arbitrage runtime {command_kind} command"
            )),
            payload_schema: json!({
                "type": "object",
                "additionalProperties": true
            }),
        })
        .collect()
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
        ExecutionIntentAck, ExecutionOrderAck, ExecutionOrderCommand, MarketDataChannel,
        MarketDataEvent, SdkResult, StrategyCommand, StrategyExecutionClient,
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
            .any(|command| command.command_kind == "rebalance"));
        assert_eq!(spec.snapshot_schema.schema_version, 1);
        assert_eq!(spec.snapshot_schema.json_schema["type"], json!("object"));
        assert!(spec.snapshot_schema.json_schema["properties"]["handled_events"].is_object());
        assert!(spec.snapshot_schema.json_schema["properties"]["market_data_events"].is_object());
        assert!(spec.snapshot_schema.json_schema["properties"]["runtime_contract"].is_object());
        assert_secret_free(&serde_json::to_value(spec).expect("spec should serialize"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_should_be_secret_free() {
        let mut runtime = SpotSpotArbitrageRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({
                "trading_mode": "paper",
                "exchanges": ["gateio", "bitget"],
                "symbols": ["BTCUSDT"],
                "max_notional_per_trade": 10.0,
                "min_notional_per_trade": 1.0,
                "max_notional_per_symbol": 100.0,
                "max_total_notional": 100.0,
                "min_net_spread_bps": 0.0,
                "min_depth_notional": 1.0,
                "market_data_mode": "websocket_cache",
                "websocket": {
                    "enabled": true,
                    "symbols": ["BTC/USDT"],
                    "exchanges": ["gate.io", "bitget"],
                    "depth": 20
                },
                "monitoring": {
                    "api_key": "must-not-leak",
                    "secret": "must-not-leak"
                }
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
        let mut runtime = SpotSpotArbitrageRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({
                "trading_mode": "paper",
                "exchanges": ["gateio", "bitget"],
                "symbols": ["BTCUSDT"],
                "max_notional_per_trade": 10.0,
                "min_notional_per_trade": 1.0,
                "max_notional_per_symbol": 100.0,
                "max_total_notional": 100.0,
                "min_net_spread_bps": 0.0,
                "min_depth_notional": 1.0,
                "market_data_mode": "websocket_cache",
                "websocket": {
                    "enabled": true,
                    "exchanges": ["gate.io", "bitget"],
                    "symbols": ["BTC/USDT"],
                    "depth": 20
                }
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        assert_eq!(runtime.market_data_subscriptions.len(), 2);
        assert_eq!(runtime.market_data_subscriptions[0].exchange_id, "gateio");
        assert_eq!(
            runtime.market_data_subscriptions[0].channels,
            vec![MarketDataChannel::OrderBookDepth]
        );

        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap();
        runtime
            .handle_event(StrategyEvent::MarketData(MarketDataEvent {
                schema_version: 1,
                exchange_id: "gateio".to_string(),
                symbol: "BTCUSDT".to_string(),
                received_at: observed_at,
                payload: json!({"best_bid": "99.0", "best_ask": "100.0"}),
            }))
            .await
            .expect("market event should apply");
        runtime
            .handle_event(StrategyEvent::MarketData(MarketDataEvent {
                schema_version: 1,
                exchange_id: "bitget".to_string(),
                symbol: "BTCUSDT".to_string(),
                received_at: observed_at,
                payload: json!({"best_bid": "101.0", "best_ask": "102.0"}),
            }))
            .await
            .expect("market event should apply");
        runtime
            .handle_event(StrategyEvent::Execution(ExecutionEvent {
                schema_version: 1,
                event_id: "exec-1".to_string(),
                client_order_id: Some("spot-spot-1".to_string()),
                occurred_at: observed_at,
                payload: json!({
                    "order_plans": [{
                        "exchange": "gate.io",
                        "symbol": "BTC/USDT",
                        "side": "buy",
                        "quantity": "0.1",
                        "notional": 10.0,
                        "intent": "dual_taker_arbitrage",
                        "client_order_id": "spot-spot-1",
                        "idempotency_key": "strategy-1:run-1:spot-spot-1",
                        "live_submission_allowed": false
                    }]
                }),
            }))
            .await
            .expect("execution event should apply");
        runtime
            .handle_event(StrategyEvent::Account(AccountEvent {
                schema_version: 1,
                account_id: "account-1".to_string(),
                received_at: observed_at,
                payload: json!({
                    "balances": [{
                        "exchange_id": "gate.io",
                        "asset": "usdt",
                        "total": 100.0,
                        "available": 90.0,
                        "locked": 10.0
                    }]
                }),
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
        assert_eq!(snapshot.payload["market_data_events"], json!(2));
        assert_eq!(snapshot.payload["execution_events"], json!(1));
        assert_eq!(snapshot.payload["account_events"], json!(1));
        assert_eq!(snapshot.payload["operator_commands"], json!(1));
        assert_eq!(snapshot.payload["configured_symbols"][0], json!("BTCUSDT"));
        assert_eq!(snapshot.payload["trading_mode"], json!("paper"));
        assert_eq!(
            snapshot.payload["runtime_contract"]["market_data"]["subscriptions"]
                .as_array()
                .expect("subscriptions should be an array")
                .len(),
            2
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["opportunity"]["candidate_pairs"][0]
                ["buy_exchange"],
            json!("bitget")
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["opportunity"]["candidate_pairs"][0]["accepted"],
            json!(false)
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["opportunity"]["candidate_pairs"][1]
                ["buy_exchange"],
            json!("gateio")
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["opportunity"]["candidate_pairs"][1]["accepted"],
            json!(true)
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["opportunity"]["candidate_pairs"][1]["buy_price"],
            json!(100.0)
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["opportunity"]["candidate_pairs"][1]["sell_price"],
            json!(101.0)
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["opportunity"]["candidate_pairs"][1]
                ["estimated_net_spread_bps"],
            json!(80.0)
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["opportunity"]["candidate_pairs"][1]
                ["executable_notional"],
            json!(10.0)
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["order_plans"][0]["intent"],
            json!("dual_taker_arbitrage")
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["inventory"]["balances"][0]["exchange"],
            json!("gateio")
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["fees"]["venue_rates"][0]["exchange"],
            json!("bitget")
        );
        assert_eq!(
            snapshot.payload["runtime_contract"]["readiness"]["market_data_ready"],
            json!(true)
        );
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
    fn migrated_core_should_calculate_net_spread() {
        let spread = calculate_spread(100.0, 101.0, 10.0, 12.0, 3.0, 5.0);

        assert_eq!(spread.raw_spread, 1.0);
        assert_eq!(spread.raw_spread_bps, 100.0);
        assert_eq!(spread.estimated_cost_bps, 30.0);
        assert_eq!(spread.net_spread_bps, 70.0);

        let invalid = calculate_spread(0.0, 101.0, 10.0, 10.0, 0.0, 0.0);
        assert_eq!(invalid.raw_spread_bps, 0.0);
        assert_eq!(invalid.net_spread_bps, -20.0);
    }

    #[test]
    fn migrated_core_should_track_summary_report() {
        let mut report = SummaryReport::default();
        report.symbols_scanned = 2;
        report.record_opportunity(&opportunity_for_test(true, None));
        report.record_opportunity(&opportunity_for_test(
            false,
            Some(RejectionReason::NetSpreadBelowThreshold),
        ));
        report.record_trade(&SimulatedTradeRecord {
            timestamp: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            buy_exchange: "gateio".to_string(),
            sell_exchange: "bitget".to_string(),
            buy_avg_price: 100.0,
            sell_avg_price: 101.0,
            quantity: 1.0,
            notional: 100.0,
            buy_fee: 0.1,
            sell_fee: 0.1,
            gross_pnl: 1.0,
            net_pnl: 0.8,
            pnl_category: TradePnlCategory::Arbitrage,
            slippage_cost: 0.0,
            capital_cost: 0.0,
            transfer_cost: 0.0,
            inventory_rebalance_cost: 0.0,
            latency_penalty_cost: 0.0,
            latency_ms: 10,
            order_book_age_ms: 20,
            execution_mode: "paper".to_string(),
        });

        assert_eq!(report.opportunities_detected, 2);
        assert_eq!(report.opportunities_accepted, 1);
        assert_eq!(report.opportunities_rejected, 1);
        assert_eq!(
            report
                .rejection_reasons
                .get(&RejectionReason::NetSpreadBelowThreshold),
            Some(&1)
        );
        assert_eq!(report.total_fees, 0.2);
        assert_eq!(report.symbol_net_pnl("BTCUSDT"), 0.8);
        assert!(report.render().contains("accepted=1"));
    }

    #[test]
    fn migrated_core_should_handle_books_and_depth() {
        let now = Utc::now();
        let fresh = CachedBook {
            exchange: "gateio".to_string(),
            symbol: "BTCUSDT".to_string(),
            bids: vec![
                SpotOrderBookLevel {
                    price: 100.0,
                    quantity: 0.5,
                },
                SpotOrderBookLevel {
                    price: 99.0,
                    quantity: 1.0,
                },
            ],
            asks: Vec::new(),
            best_bid: Some(100.0),
            best_ask: Some(101.0),
            exchange_timestamp: Some(now),
            local_timestamp: now - chrono::Duration::milliseconds(500),
            latency_ms: Some(10),
            sequence: Some(1),
            source: BookSource::Websocket,
            is_stale: false,
        };

        assert!(fresh.is_fresh(now, 500));
        assert!(!fresh.is_fresh(now, 499));
        assert_eq!(depth_notional(&fresh.bids, 120.0), 120.0);
        assert_eq!(depth_notional(&fresh.bids, 1_000.0), 149.0);
    }

    #[test]
    fn migrated_core_should_select_configured_pair() {
        let pair = configured_spot_pair(&["gate.io".to_string(), "bitget".to_string()]);
        assert_eq!(pair, (SpotVenue::GateIo, SpotVenue::Bitget));
        let fallback = configured_spot_pair(&["mexc".to_string(), "coinex".to_string()]);
        assert_eq!(fallback, (SpotVenue::Mexc, SpotVenue::CoinEx));
        assert_eq!(SpotVenue::GateIo.other(), SpotVenue::Bitget);
    }

    #[test]
    fn event_driven_spread_engine_should_recompute_only_updated_symbol_pairs() {
        let mut engine = EventDrivenSpreadEngine::new(EventDrivenSpreadEngineConfig {
            exchanges: vec![
                "mexc".to_string(),
                "coinex".to_string(),
                "bitget".to_string(),
            ],
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        });
        let now = Utc::now();

        let first = engine.on_book_event(SpotBookEvent::snapshot(
            "mexc", "BTCUSDT", 99.0, 100.0, 1.0, now,
        ));
        assert!(first.recomputed_pairs.is_empty());

        let second = engine.on_book_event(SpotBookEvent::snapshot(
            "coinex", "BTCUSDT", 101.0, 102.0, 1.0, now,
        ));
        assert_eq!(
            second.recomputed_pairs,
            vec![
                DirectedVenuePair {
                    buy_exchange: "coinex".to_string(),
                    sell_exchange: "mexc".to_string(),
                },
                DirectedVenuePair {
                    buy_exchange: "mexc".to_string(),
                    sell_exchange: "coinex".to_string(),
                },
            ]
        );

        engine.on_book_event(SpotBookEvent::snapshot(
            "bitget", "ETHUSDT", 199.0, 200.0, 1.0, now,
        ));
        let eth_update = engine.on_book_event(SpotBookEvent::snapshot(
            "mexc", "ETHUSDT", 198.0, 199.0, 1.0, now,
        ));
        assert_eq!(
            eth_update
                .recomputed_pairs
                .iter()
                .map(|pair| (&pair.buy_exchange, &pair.sell_exchange))
                .collect::<Vec<_>>(),
            vec![
                (&"bitget".to_string(), &"mexc".to_string()),
                (&"mexc".to_string(), &"bitget".to_string()),
            ]
        );
    }

    #[test]
    fn event_driven_spread_engine_should_not_recompute_from_stale_books() {
        let mut engine = EventDrivenSpreadEngine::new(EventDrivenSpreadEngineConfig {
            exchanges: vec!["mexc".to_string(), "coinex".to_string()],
            symbols: vec!["BTCUSDT".to_string()],
        });
        let now = Utc::now();

        engine.on_book_event(SpotBookEvent::snapshot(
            "mexc", "BTCUSDT", 99.0, 100.0, 1.0, now,
        ));
        let ready = engine.on_book_event(SpotBookEvent::snapshot(
            "coinex", "BTCUSDT", 101.0, 102.0, 1.0, now,
        ));
        assert_eq!(ready.recomputed_pairs.len(), 2);

        let stale = engine.on_book_event(SpotBookEvent::stale(
            "coinex",
            "BTCUSDT",
            "heartbeat timeout",
            now,
        ));
        assert!(stale.recomputed_pairs.is_empty());
        assert!(engine.book("coinex", "BTCUSDT").unwrap().is_stale);

        let update = engine.on_book_event(SpotBookEvent::snapshot(
            "mexc", "BTCUSDT", 100.0, 101.0, 1.0, now,
        ));
        assert!(update.recomputed_pairs.is_empty());
    }

    #[test]
    fn event_driven_spread_engine_should_ignore_unconfigured_symbol_and_exchange() {
        let mut engine = EventDrivenSpreadEngine::new(EventDrivenSpreadEngineConfig {
            exchanges: vec!["mexc".to_string(), "coinex".to_string()],
            symbols: vec!["BTCUSDT".to_string()],
        });
        let now = Utc::now();

        engine.on_book_event(SpotBookEvent::snapshot(
            "mexc", "BTCUSDT", 99.0, 100.0, 1.0, now,
        ));
        let wrong_symbol = engine.on_book_event(SpotBookEvent::snapshot(
            "coinex", "ETHUSDT", 101.0, 102.0, 1.0, now,
        ));
        assert!(wrong_symbol.recomputed_pairs.is_empty());

        let wrong_exchange = engine.on_book_event(SpotBookEvent::snapshot(
            "gateio", "BTCUSDT", 101.0, 102.0, 1.0, now,
        ));
        assert!(wrong_exchange.recomputed_pairs.is_empty());
        assert!(engine.book("gateio", "BTCUSDT").is_none());
    }

    #[test]
    fn migrated_core_should_track_risk_state_limits_and_cooldowns() {
        let now = Utc::now();
        let limits = SpotRiskLimits {
            max_notional_per_symbol: 150.0,
            max_total_notional: 200.0,
            max_daily_loss: 5.0,
            max_trade_loss: 2.0,
            max_consecutive_rejections: 2,
        };
        let mut risk = SpotRiskState::default();

        assert!(!risk.notional_limit_hit(&limits, "BTCUSDT", 100.0));
        assert!(risk.notional_limit_hit(&limits, "BTCUSDT", 201.0));
        assert!(!spot_rejection_counts_toward_consecutive(
            RejectionReason::ExchangeHealth
        ));
        assert!(spot_rejection_counts_toward_consecutive(
            RejectionReason::PaperExecutionRejected
        ));

        risk.record_rejection("BTCUSDT", RejectionReason::PaperExecutionRejected, now);
        risk.record_rejection("BTCUSDT", RejectionReason::NotionalLimit, now);
        assert_eq!(risk.consecutive_rejections(), 2);
        assert!(risk.consecutive_rejection_limit_hit(&limits));

        let losing_trade = simulated_trade_for_test("BTCUSDT", 120.0, -6.0);
        assert!(risk.trade_loss_limit_hit(&limits, &losing_trade));
        risk.record_trade(&losing_trade, now);
        assert_eq!(risk.consecutive_rejections(), 0);
        assert_eq!(risk.total_notional(), 120.0);
        assert_eq!(risk.symbol_notional("BTCUSDT"), 120.0);
        assert_eq!(risk.daily_pnl(), -6.0);
        assert!(risk.daily_loss_limit_hit(&limits));
        assert!(risk.notional_limit_hit(&limits, "BTCUSDT", 31.0));
        assert!(risk.notional_limit_hit(&limits, "ETHUSDT", 81.0));

        risk.apply_trade_cooldown("BTCUSDT", 5_000, now);
        assert!(risk.is_in_cooldown("BTCUSDT", now + chrono::Duration::milliseconds(4_999)));
        assert!(!risk.is_in_cooldown("BTCUSDT", now + chrono::Duration::milliseconds(5_000)));
    }

    #[test]
    fn migrated_core_should_blacklist_after_repeated_exchange_health_failures() {
        let now = Utc::now();
        let mut risk = SpotRiskState::default();

        for attempt in 0..19 {
            risk.record_rejection(
                "BTCUSDT",
                RejectionReason::ExchangeHealth,
                now + chrono::Duration::milliseconds(attempt),
            );
        }
        assert!(!risk.is_symbol_blacklisted("BTCUSDT", now));

        risk.record_rejection(
            "BTCUSDT",
            RejectionReason::ExchangeHealth,
            now + chrono::Duration::milliseconds(20),
        );
        assert!(risk.is_symbol_blacklisted("BTCUSDT", now + chrono::Duration::seconds(60)));
        assert!(
            !risk.is_symbol_blacklisted("BTCUSDT", now + chrono::Duration::milliseconds(300_021))
        );

        risk.record_trade(&simulated_trade_for_test("BTCUSDT", 10.0, 1.0), now);
        assert!(!risk.is_symbol_blacklisted("BTCUSDT", now + chrono::Duration::seconds(60)));
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

    fn simulated_trade_for_test(symbol: &str, notional: f64, net_pnl: f64) -> SimulatedTradeRecord {
        SimulatedTradeRecord {
            timestamp: Utc::now(),
            symbol: symbol.to_string(),
            buy_exchange: "gateio".to_string(),
            sell_exchange: "bitget".to_string(),
            buy_avg_price: 100.0,
            sell_avg_price: 101.0,
            quantity: notional / 100.0,
            notional,
            buy_fee: 0.1,
            sell_fee: 0.1,
            gross_pnl: net_pnl + 0.2,
            net_pnl,
            pnl_category: TradePnlCategory::Arbitrage,
            slippage_cost: 0.0,
            capital_cost: 0.0,
            transfer_cost: 0.0,
            inventory_rebalance_cost: 0.0,
            latency_penalty_cost: 0.0,
            latency_ms: 10,
            order_book_age_ms: 20,
            execution_mode: "paper".to_string(),
        }
    }

    fn opportunity_for_test(
        accepted: bool,
        rejection_reason: Option<RejectionReason>,
    ) -> OpportunityRecord {
        OpportunityRecord {
            timestamp: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            buy_exchange: "gateio".to_string(),
            sell_exchange: "bitget".to_string(),
            buy_price: 100.0,
            sell_price: 101.0,
            raw_spread_bps: 100.0,
            buy_fee_bps: 10.0,
            sell_fee_bps: 10.0,
            fee_source_buy: SpotFeeSource::Config,
            fee_source_sell: SpotFeeSource::Config,
            platform_discount_applied: false,
            estimated_fee_bps: 20.0,
            estimated_slippage_bps: 2.0,
            safety_buffer_bps: 3.0,
            estimated_net_spread_bps: 75.0,
            estimated_total_fee: 0.2,
            estimated_gross_pnl: 1.0,
            estimated_net_pnl: 0.8,
            capital_cost_bps: 0.0,
            transfer_cost_bps: 0.0,
            transfer_delay_penalty_bps: 0.0,
            inventory_rebalance_cost_bps: 0.0,
            latency_penalty_bps: 0.0,
            effective_min_net_spread_bps: 0.0,
            estimated_slippage_cost: 0.0,
            estimated_capital_cost: 0.0,
            estimated_transfer_cost: 0.0,
            estimated_inventory_rebalance_cost: 0.0,
            estimated_latency_penalty_cost: 0.0,
            estimated_total_cost: 0.2,
            executable_notional: 100.0,
            quantity: 1.0,
            accepted,
            rejection_reason,
            rejection_detail: None,
            buy_book_age_ms: 10,
            sell_book_age_ms: 20,
            buy_book_source: BookSource::Websocket,
            sell_book_source: BookSource::Rest,
            buy_latency_ms: Some(5),
            sell_latency_ms: Some(7),
        }
    }
}
