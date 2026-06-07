use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, MarketDataChannel, MarketType, RequiredAccountPermission, RiskCapability,
    RiskCapabilityDeclaration, StrategyConfigSchema, StrategyContext, StrategyEvent,
    StrategyInstanceId, StrategyRuntime, StrategySnapshot, StrategySnapshotSchema, StrategySpec,
    StrategyStatus,
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
pub const MIGRATED_FROM: &str = "src/strategies/mean_reversion";

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
    pub symbols: Vec<String>,
    pub exchange: String,
    pub lookback_window: u32,
    pub entry_zscore: f64,
    pub max_notional_quote: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeanReversionSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
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
        }
    }

    fn snapshot_payload(&self) -> MeanReversionSnapshotPayload {
        MeanReversionSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
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
            "Partially migrated mean reversion strategy with adapter-free planning core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: Vec::new(),
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
                json!(["engine", "types", "utils"]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!(["strategy", "tasks", "execution", "data"]),
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
