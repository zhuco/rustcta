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

pub mod engine;
pub mod types;
pub mod utils;

pub use engine::{
    compute_indicators, evaluate_range_conditions, plan_symbol_orders, IndicatorOutputs,
    LiquiditySnapshot, MeanReversionEngineConfig, OrderPlan, PlannedOrders, SymbolConfig,
    SymbolMeta, SymbolSnapshot,
};

pub const STRATEGY_KIND: &str = "mean_reversion";
pub const DISPLAY_NAME: &str = "Mean Reversion";
pub const MIGRATED_FROM: &str = "legacy-strategy:mean_reversion";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeanReversionStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for MeanReversionStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeanReversionConfig {
    #[serde(default = "default_mean_reversion_symbols")]
    pub symbols: Vec<String>,
    #[serde(default = "default_mean_reversion_exchange")]
    pub exchange: String,
    #[serde(default = "default_mean_reversion_lookback")]
    pub lookback_window: u32,
    #[serde(default = "default_mean_reversion_entry_zscore")]
    pub entry_zscore: f64,
    #[serde(default = "default_mean_reversion_notional")]
    pub max_notional_quote: String,
    #[serde(default = "default_mean_reversion_candle_interval")]
    pub candle_interval: String,
    #[serde(default = "default_mean_reversion_market_type")]
    pub market_type: MarketType,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeanReversionSnapshotPayload {
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
}

#[derive(Debug, Clone)]
pub struct MeanReversionRuntime {
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
    config: Option<MeanReversionConfig>,
    market_data_subscriptions: Vec<MarketDataSubscription>,
}

impl MeanReversionRuntime {
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

    fn snapshot_payload(&self) -> MeanReversionSnapshotPayload {
        MeanReversionSnapshotPayload {
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
        }
    }
}

impl Default for MeanReversionRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for MeanReversionRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        let config: MeanReversionConfig = serde_json::from_value(ctx.config().clone())?;
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.last_event_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.market_data_subscriptions = mean_reversion_market_data_subscriptions(&config);
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
            "Mean reversion strategy SDK contract with adapter-free planning core.".to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places mean reversion entries and exits",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels stale mean reversion orders",
            ),
        ],
        market_data_subscriptions: mean_reversion_market_data_subscriptions(
            &MeanReversionConfig::default(),
        ),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadOrders,
            AccountPermission::TradeSpot,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            ("legacy_module".to_string(), json!(MIGRATED_FROM)),
            ("partial_core_migration".to_string(), json!(false)),
            ("runtime_contract_migration".to_string(), json!(true)),
            (
                "migrated_core_modules".to_string(),
                json!(["engine", "types", "utils"]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!(["legacy_exchange_io", "runtime_orchestration"]),
            ),
            (
                "market_data_channels".to_string(),
                json!([MarketDataChannel::Candles {
                    interval: "configured".to_string()
                }]),
            ),
            ("primary_market_type".to_string(), json!(MarketType::Spot)),
        ]),
    }
}

impl Default for MeanReversionConfig {
    fn default() -> Self {
        Self {
            symbols: default_mean_reversion_symbols(),
            exchange: default_mean_reversion_exchange(),
            lookback_window: default_mean_reversion_lookback(),
            entry_zscore: default_mean_reversion_entry_zscore(),
            max_notional_quote: default_mean_reversion_notional(),
            candle_interval: default_mean_reversion_candle_interval(),
            market_type: MarketType::Spot,
            dry_run: true,
        }
    }
}

pub fn mean_reversion_market_data_subscriptions(
    config: &MeanReversionConfig,
) -> Vec<MarketDataSubscription> {
    let candle_interval = config.candle_interval.trim();
    let mut channels = vec![MarketDataChannel::OrderBookTop];
    if !candle_interval.is_empty() {
        channels.push(MarketDataChannel::Candles {
            interval: candle_interval.to_string(),
        });
    }

    config
        .symbols
        .iter()
        .filter(|symbol| !symbol.trim().is_empty())
        .map(|symbol| MarketDataSubscription {
            exchange_id: config.exchange.clone(),
            symbol: symbol.trim().to_string(),
            market_type: config.market_type.clone(),
            channels: channels.clone(),
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn mean_reversion_order_plan_to_execution_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    risk_profile_id: &str,
    client_order_id: String,
    plan: &OrderPlan,
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
        exchange_id: exchange_id.to_string(),
        symbol: plan.symbol.clone(),
        side: plan.side.clone(),
        order_type: OrderType::Limit,
        quantity: plan.quantity.to_string(),
        price: Some(plan.limit_price.to_string()),
        time_in_force: Some(TimeInForce::GoodTilCanceled),
        reduce_only: false,
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            (
                "source_plan".to_string(),
                json!("mean_reversion_order_plan"),
            ),
            ("stop_price".to_string(), json!(plan.stop_price)),
            ("take_profit".to_string(), json!(plan.take_profit)),
        ]),
    }
}

pub fn mean_reversion_cancel_command(
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
                "exchange",
                "lookback_window",
                "entry_zscore",
                "max_notional_quote"
            ],
            "properties": {
                "symbols": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "exchange": { "type": "string", "minLength": 1 },
                "market_type": {
                    "type": "string",
                    "enum": ["spot", "margin", "perpetual", "futures", "option"]
                },
                "candle_interval": {
                    "type": "string",
                    "minLength": 1,
                    "default": "5m"
                },
                "lookback_window": { "type": "integer", "minimum": 2 },
                "entry_zscore": { "type": "number", "exclusiveMinimum": 0.0 },
                "max_notional_quote": {
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
            "market_data_subscriptions": { "type": "array" }
        }
    })
}

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    ["pause", "resume", "stop", "refresh_indicators"]
        .into_iter()
        .map(|command_kind| StrategyCommandSchema {
            command_kind: command_kind.to_string(),
            description: Some(format!("Mean reversion runtime {command_kind} command")),
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

fn execution_idempotency_key(ctx: &StrategyContext, client_order_id: &str) -> String {
    format!("{}:{}:{}", ctx.strategy_id(), ctx.run_id(), client_order_id)
}

fn default_mean_reversion_symbols() -> Vec<String> {
    vec!["BTC/USDT".to_string()]
}

fn default_mean_reversion_exchange() -> String {
    "binance".to_string()
}

fn default_mean_reversion_lookback() -> u32 {
    20
}

fn default_mean_reversion_entry_zscore() -> f64 {
    1.2
}

fn default_mean_reversion_notional() -> String {
    "20".to_string()
}

fn default_mean_reversion_candle_interval() -> String {
    "5m".to_string()
}

fn default_mean_reversion_market_type() -> MarketType {
    MarketType::Spot
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
        assert_secret_free(&serde_json::to_value(spec).expect("spec should serialize"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_should_be_secret_free() {
        let mut runtime = MeanReversionRuntime::new();
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
    fn migrated_engine_should_plan_long_entry_without_legacy_root() {
        let config = engine_config_for_test();
        let snapshot = symbol_snapshot_for_test(82.0);
        let liquidity = LiquiditySnapshot {
            bid_price: 81.9,
            ask_price: 82.1,
            spread: 0.2,
            total_bid_depth: 10_000.0,
            total_ask_depth: 10_000.0,
            maker_fee: Some(0.0),
            timestamp: Utc::now(),
        };
        let meta = symbol_meta_for_test();

        let planned = plan_symbol_orders(&config, &snapshot, &liquidity, &meta)
            .expect("planner should evaluate symbol");

        assert!(planned.range_score >= 2);
        let long_plan = planned
            .plans
            .iter()
            .find(|plan| plan.side == rustcta_strategy_sdk::OrderSide::Buy)
            .expect("oversold input should create a long plan");
        assert_eq!(long_plan.symbol, "BTCUSDT");
        assert!(long_plan.quantity > 0.0);
        assert!(long_plan.limit_price > 0.0);
        assert!(long_plan.stop_price < long_plan.limit_price);
        assert!(long_plan.take_profit > long_plan.limit_price);
    }

    #[test]
    fn migrated_engine_should_reject_insufficient_depth() {
        let config = engine_config_for_test();
        let snapshot = symbol_snapshot_for_test(82.0);
        let liquidity = LiquiditySnapshot {
            bid_price: 81.9,
            ask_price: 82.1,
            spread: 0.2,
            total_bid_depth: 1.0,
            total_ask_depth: 1.0,
            maker_fee: Some(0.0),
            timestamp: Utc::now(),
        };
        let meta = symbol_meta_for_test();

        let planned = plan_symbol_orders(&config, &snapshot, &liquidity, &meta)
            .expect("planner should evaluate symbol");

        assert!(planned.plans.is_empty());
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

    fn engine_config_for_test() -> MeanReversionEngineConfig {
        let mut config = MeanReversionEngineConfig::default();
        config.indicators.adx.threshold = 100.0;
        config.indicators.bbw.percentile = 1.0;
        config.indicators.slope.threshold = 100.0;
        config.indicators.bollinger.entry_band_pct_long = 0.2;
        config.indicators.rsi.long_threshold = 60.0;
        config.liquidity.min_recent_quote_volume = 0.0;
        config.liquidity.depth_multiplier = 1.0;
        config.risk.per_trade_notional = 25.0;
        config.risk.per_trade_risk = 25.0;
        config
    }

    fn symbol_meta_for_test() -> SymbolMeta {
        SymbolMeta {
            symbol: "BTCUSDT".to_string(),
            tick_size: 0.1,
            step_size: 0.001,
            min_notional: Some(5.0),
            min_order_size: 0.001,
            max_order_size: 100.0,
            price_precision: 1,
            amount_precision: 3,
        }
    }

    fn symbol_snapshot_for_test(last_close: f64) -> SymbolSnapshot {
        let mut five_minute = Vec::new();
        let mut fifteen_minute = Vec::new();
        let mut one_hour = Vec::new();
        for idx in 0..24 {
            let close = if idx == 23 { last_close } else { 100.0 };
            five_minute.push(test_kline("5m", idx, close));
            fifteen_minute.push(test_kline("15m", idx, close));
            one_hour.push(test_kline("1h", idx, close));
        }

        SymbolSnapshot {
            config: SymbolConfig {
                symbol: "BTCUSDT".to_string(),
                enabled: true,
                min_quote_volume_5m: 0.0,
                depth_multiplier: 1.0,
                depth_levels: 5,
                allow_short: Some(false),
            },
            five_minute,
            fifteen_minute,
            one_hour,
            bbw_history: vec![0.5; 20],
            mid_history: vec![100.0; 20],
            sigma_history: vec![20.0; 20],
            frozen: false,
        }
    }

    fn test_kline(interval: &str, idx: usize, close: f64) -> types::Kline {
        let open_time = Utc::now() + chrono::Duration::minutes(idx as i64);
        types::Kline {
            symbol: "BTCUSDT".to_string(),
            interval: interval.to_string(),
            open_time,
            close_time: open_time + chrono::Duration::minutes(1),
            open: close,
            high: close + 1.0,
            low: (close - 1.0).max(1.0),
            close,
            volume: 100.0,
            quote_volume: 10_000.0,
            trade_count: 100,
        }
    }
}
