use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, RequiredAccountPermission, RiskCapability, RiskCapabilityDeclaration,
    StrategyConfigSchema, StrategyContext, StrategyEvent, StrategyInstanceId, StrategyRuntime,
    StrategySnapshot, StrategySnapshotSchema, StrategySpec, StrategyStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod core;

pub use core::{
    CanonicalSymbol, ExchangeFeeRates, ExchangeId, FeeBreakdown, FeeModel, FeeRole,
    FillInferenceType, FundingEstimate, FundingModel, FundingSettlementLedger, MakerLegKind,
    OrderSide, PositionSide, SimulatedBundleState, SimulatedBundleStatus, StrategyRoute,
};

pub const STRATEGY_KIND: &str = "cross_exchange_arbitrage";
pub const DISPLAY_NAME: &str = "Cross Exchange Arbitrage";
pub const MIGRATED_FROM: &str = "src/strategies/cross_exchange_arbitrage";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossExchangeArbitrageStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for CrossExchangeArbitrageStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossExchangeArbitrageConfig {
    pub venues: Vec<String>,
    pub symbols: Vec<String>,
    pub min_profit_bps: f64,
    pub max_position_notional_quote: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossExchangeArbitrageSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct CrossExchangeArbitrageRuntime {
    instance_id: StrategyInstanceId,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
}

impl CrossExchangeArbitrageRuntime {
    pub fn new() -> Self {
        Self {
            instance_id: StrategyInstanceId::new("unstarted"),
            strategy_id: STRATEGY_KIND.to_string(),
            run_id: "unstarted".to_string(),
            status: StrategyStatus::Stopped,
            started_at: None,
            last_event_at: None,
            handled_events: 0,
        }
    }

    fn snapshot_payload(&self) -> CrossExchangeArbitrageSnapshotPayload {
        CrossExchangeArbitrageSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
        }
    }
}

impl Default for CrossExchangeArbitrageRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for CrossExchangeArbitrageRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.status = StrategyStatus::Stopped;
        Ok(())
    }

    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()> {
        self.handled_events += 1;
        self.last_event_at = Some(event_timestamp(&event));
        if matches!(event, StrategyEvent::Stopping(_)) {
            self.status = StrategyStatus::Stopping;
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
            "Partially migrated cross-exchange arbitrage strategy with adapter-free domain core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: Vec::new(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places cross-venue arbitrage orders",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels stale arbitrage orders",
            ),
            risk_capability(
                RiskCapability::CrossAccountRead,
                "Reads inventory and order state across venues",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Reserves inventory for paired execution",
            ),
        ],
        market_data_subscriptions: Vec::new(),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadOrders,
            AccountPermission::ReadFills,
            AccountPermission::TradeSpot,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            ("legacy_module".to_string(), json!(MIGRATED_FROM)),
            ("partial_core_migration".to_string(), json!(true)),
            (
                "migrated_core_modules".to_string(),
                json!(["state", "fee_model", "funding_model", "settlement_ledger"]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!([
                    "config",
                    "runtime",
                    "tasks",
                    "execution",
                    "market_data",
                    "private_sync",
                    "storage"
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
            "additionalProperties": false,
            "required": [
                "venues",
                "symbols",
                "min_profit_bps",
                "max_position_notional_quote"
            ],
            "properties": {
                "venues": {
                    "type": "array",
                    "minItems": 2,
                    "items": { "type": "string", "minLength": 1 }
                },
                "symbols": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "min_profit_bps": { "type": "number", "minimum": 0.0 },
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
            "started_at": { "type": ["string", "null"], "format": "date-time" },
            "last_event_at": { "type": ["string", "null"], "format": "date-time" }
        }
    })
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
        let mut runtime = CrossExchangeArbitrageRuntime::new();
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
            "src/exchanges",
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
    fn migrated_core_should_model_routes_and_sides() {
        let route = StrategyRoute {
            long_exchange: ExchangeId::new("binance"),
            short_exchange: ExchangeId::new("bitget"),
            maker_exchange: ExchangeId::new("binance"),
            taker_exchange: ExchangeId::new("bitget"),
            maker_side: OrderSide::Buy,
            taker_side: OrderSide::Sell,
            maker_leg_kind: MakerLegKind::LongMakerBuy,
        };
        let state = SimulatedBundleState {
            bundle_id: "bundle-1".to_string(),
            opportunity_id: "opp-1".to_string(),
            status: SimulatedBundleStatus::Observing,
            route,
            target_notional_usdt: 100.0,
            opened_at: None,
            updated_at: Utc::now(),
        };

        assert_eq!(OrderSide::Buy.opposite(), OrderSide::Sell);
        assert_eq!(state.route.long_exchange.as_str(), "binance");
        assert_eq!(state.route.taker_side, OrderSide::Sell);
    }

    #[test]
    fn migrated_core_fee_model_should_allow_negative_maker_fee() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            ExchangeId::new("binance"),
            ExchangeFeeRates {
                maker: -0.0001,
                taker: 0.0005,
            },
        );
        let model = FeeModel::new(
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
            overrides,
        );

        assert_eq!(
            model.fee_amount(&ExchangeId::new("binance"), FeeRole::Maker, 100.0),
            -0.01
        );
        let breakdown = model.estimate_maker_taker_round_trip(
            &ExchangeId::new("binance"),
            &ExchangeId::new("bitget"),
            100.0,
        );
        assert!((breakdown.open_fee() - 0.04).abs() < 1e-9);
        assert!((breakdown.total_normal_fee() - 0.08).abs() < 1e-9);
    }

    #[test]
    fn migrated_core_funding_should_apply_long_short_direction() {
        let model = FundingModel::default();
        let estimate = model.estimate_pair(100.0, 0.0003, 100.0, 0.0005);

        assert!((estimate.long_leg_funding + 0.03).abs() < 1e-9);
        assert!((estimate.short_leg_funding - 0.05).abs() < 1e-9);
        assert!((estimate.net_funding - 0.02).abs() < 1e-9);

        let adverse = model.estimate_pair(100.0, 0.001, 100.0, -0.001);
        assert!(adverse.net_funding < 0.0);
        assert!(adverse.dangerous);
    }

    #[test]
    fn migrated_core_funding_should_record_settlement_pnl() {
        let mut ledger = FundingSettlementLedger::default();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let now = Utc::now();

        ledger.record(core::FundingModel::settle_leg(
            "bundle-1",
            ExchangeId::new("binance"),
            symbol.clone(),
            PositionSide::Long,
            100.0,
            0.0003,
            Some(65_000.0),
            now,
        ));
        ledger.record(core::FundingModel::settle_leg(
            "bundle-1",
            ExchangeId::new("bitget"),
            symbol,
            PositionSide::Short,
            100.0,
            0.0005,
            Some(65_010.0),
            now,
        ));

        assert_eq!(ledger.settlements().len(), 2);
        assert!((ledger.total_pnl_for_bundle("bundle-1") - 0.02).abs() < 1e-9);
        assert!((ledger.total_pnl_usdt() - 0.02).abs() < 1e-9);
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
}
