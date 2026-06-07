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
    adopt_short_progress, build_short_ladder_client_order_id_at, capped_layer_notional,
    cumulative_layer_notionals, decimal_places_from_step, derive_order_quantity_from_precision,
    held_bars_since, infer_short_ladder_last_price, is_short_ladder_exit_order,
    layer_notionals_until, maker_buy_price, maker_sell_price, matches_short_position_side,
    order_params_with_precision, position_params, precision_format, precision_round_down,
    precision_round_up, same_pending_initial_order, should_add_short_layer,
    should_fallback_initial_entry_to_market, AccountConfig, AdoptedShortProgress, DataConfig,
    ExecutionConfig, LadderConfig, LiveShortPosition, LiveTakeProfitMode, NotificationConfig,
    ObservedOrder, PendingEntryOrder, PendingExitOrder, PendingInitialEntry, RuntimeState,
    ShortLadderLiveCoreConfig, SignalConfig, StrategyInfo, SymbolConfig, SymbolPrecision,
    SymbolState, WebSocketExitConfig,
};

pub const STRATEGY_KIND: &str = "short_ladder_live";
pub const DISPLAY_NAME: &str = "Short Ladder Live";
pub const MIGRATED_FROM: &str = "src/strategies/short_ladder_live";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShortLadderLiveStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for ShortLadderLiveStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShortLadderLiveConfig {
    pub symbols: Vec<String>,
    pub initial_notional: String,
    pub max_notional: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShortLadderLiveSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ShortLadderLiveRuntime {
    instance_id: StrategyInstanceId,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
}

impl ShortLadderLiveRuntime {
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

    fn snapshot_payload(&self) -> ShortLadderLiveSnapshotPayload {
        ShortLadderLiveSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
        }
    }
}

impl Default for ShortLadderLiveRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for ShortLadderLiveRuntime {
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
            "Partially migrated short ladder live strategy with adapter-free ladder model core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: Vec::new(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places short ladder entry orders",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels pending ladder orders",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Caps short ladder notional per configured symbol",
            ),
        ],
        market_data_subscriptions: Vec::new(),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadPositions,
            AccountPermission::ReadOrders,
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
                    "ladder_config",
                    "symbol_state",
                    "short_position_model",
                    "adopted_short_progress",
                    "layer_notional_budget",
                    "precision_helpers",
                    "trailing_take_profit",
                    "execution_sizing_helpers",
                    "pending_order_matching",
                    "client_order_id_helpers"
                ]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!([
                    "market_signal",
                    "execution",
                    "tasks",
                    "websocket",
                    "logging",
                    "runtime_orchestration"
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
            "required": ["symbols", "initial_notional", "max_notional"],
            "properties": {
                "symbols": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "initial_notional": {
                    "type": "string",
                    "pattern": "^[0-9]+(\\.[0-9]+)?$"
                },
                "max_notional": {
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
    use chrono::TimeZone;
    use rustcta_strategy_sdk::{
        ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
        ExecutionOrderAck, ExecutionOrderCommand, MarketType, OrderSide, SdkResult,
        StrategyExecutionClient,
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
        let mut runtime = ShortLadderLiveRuntime::new();
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
                "symbols": ["ENAUSDC"]
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
    fn migrated_config_should_accept_atr_trailing_take_profit_and_entry_fallback() {
        let yaml = r#"
strategy:
  name: short_ladder_live_test
  version: 0.1.0
account:
  account_id: account-1
data: {}
symbols:
  - symbol: ENAUSDC
ladder:
  take_profit_mode: atr_trailing
  trailing_take_profit_activation_atr: 1.4
  trailing_take_profit_distance_atr: 1.4
execution:
  initial_order_taker_fallback_secs: 45
"#;

        let config: ShortLadderLiveCoreConfig =
            serde_yaml::from_str(yaml).expect("config should parse");

        assert_eq!(config.account.market_type, MarketType::Futures);
        assert_eq!(config.symbols[0].initial_notional, 200.0);
        assert_eq!(
            config.ladder.take_profit_mode,
            LiveTakeProfitMode::AtrTrailing
        );
        assert_eq!(config.execution.initial_order_taker_fallback_secs, Some(45));
        assert_eq!(config.notifications.wechat_webhook_url, None);
    }

    #[test]
    fn migrated_model_should_map_adopted_short_progress_to_ladder_layer() {
        let weights = vec![1.0, 1.0, 3.0, 5.0];
        let l1 = adopt_short_progress(2.0, 100.0, 200.0, 2000.0, &weights, 0.02);
        let l2 = adopt_short_progress(4.0, 100.0, 200.0, 2000.0, &weights, 0.02);
        let l3 = adopt_short_progress(7.2, 100.0, 200.0, 2000.0, &weights, 0.02);
        let l4 = adopt_short_progress(20.0, 100.0, 200.0, 2000.0, &weights, 0.02);

        assert_eq!(l1.filled_layers, 1);
        assert_eq!(l1.next_layer_index, 1);
        assert_eq!(l2.filled_layers, 2);
        assert_eq!(l2.next_layer_index, 2);
        assert_eq!(l3.filled_layers, 3);
        assert_eq!(l3.next_layer_index, 3);
        assert_eq!(l4.filled_layers, 4);
        assert_eq!(l4.next_layer_index, 4);
        assert!(l4.capped_at_max_notional);
    }

    #[test]
    fn migrated_model_should_infer_last_ladder_price_from_average() {
        let notionals = vec![200.0, 200.0, 600.0];
        let spacing = 1.5;
        let prices = [100.0, 101.5, 103.0];
        let total_notional = notionals.iter().sum::<f64>();
        let qty = notionals
            .iter()
            .zip(prices.iter())
            .map(|(notional, price)| notional / price)
            .sum::<f64>();
        let average = total_notional / qty;

        let last = infer_short_ladder_last_price(average, spacing, &notionals);

        assert!((last - 103.0).abs() < 1e-6);
    }

    #[test]
    fn migrated_model_should_add_layer_only_when_short_is_losing() {
        let position = sample_short_position();

        assert_eq!(
            should_add_short_layer(&position, 100.5, 4, 0.55, true),
            None
        );
        assert_eq!(
            should_add_short_layer(&position, 101.5, 4, 0.55, true),
            None
        );
        assert_eq!(
            should_add_short_layer(&position, 102.1, 4, 0.55, true),
            Some((2, 102.1))
        );
        assert_eq!(
            should_add_short_layer(&position, 102.1, 3, 0.55, false),
            None
        );
    }

    #[test]
    fn migrated_model_should_cap_layer_notional_by_symbol_max() {
        let weights = vec![1.0, 1.0, 3.0, 5.0];

        assert_eq!(
            capped_layer_notional(200.0, 2000.0, 1000.0, &weights, 3),
            Some(1000.0)
        );
        assert_eq!(
            capped_layer_notional(200.0, 1800.0, 1000.0, &weights, 3),
            Some(800.0)
        );
        assert_eq!(
            capped_layer_notional(200.0, 2000.0, 2000.0, &weights, 3),
            None
        );
    }

    #[test]
    fn migrated_model_should_floor_completed_hold_bars() {
        let opened_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let reference = Utc.with_ymd_and_hms(2026, 1, 1, 1, 4, 59).unwrap();

        assert_eq!(held_bars_since(opened_at, reference, 300), 12);
    }

    #[test]
    fn migrated_precision_helpers_should_match_exchange_steps() {
        assert!((precision_round_up(10.001, 0.01) - 10.01).abs() < 1e-9);
        assert!((precision_round_up(10.0, 0.01) - 10.0).abs() < 1e-9);
        assert!((precision_round_down(10.009, 0.01) - 10.0).abs() < 1e-9);
        assert_eq!(decimal_places_from_step(0.001), 3);
        assert_eq!(precision_format(10.010, 0.001), "10.01");
    }

    #[test]
    fn migrated_model_should_activate_short_trailing_take_profit_then_exit_on_rebound() {
        let opened_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut position = LiveShortPosition {
            average_entry_price: 100.0,
            quantity: 2.0,
            current_notional: 200.0,
            filled_layers: 1,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            breakeven_armed: false,
            trailing_take_profit_armed: false,
            best_favorable_price: None,
            opened_at,
            last_sync_at: opened_at,
        };

        assert_eq!(
            position.update_short_trailing_take_profit(98.0, 1.4, 1.4),
            None
        );
        assert_eq!(
            position.update_short_trailing_take_profit(97.0, 1.4, 1.4),
            None
        );
        assert!(position.trailing_take_profit_armed);
        assert_eq!(position.best_favorable_price, Some(97.0));
        assert_eq!(
            position.update_short_trailing_take_profit(96.0, 1.4, 1.4),
            None
        );
        assert_eq!(position.best_favorable_price, Some(96.0));
        assert_eq!(
            position.update_short_trailing_take_profit(98.9, 1.4, 1.4),
            Some(98.8)
        );
    }

    #[test]
    fn migrated_model_should_match_dual_position_params() {
        assert!(matches_short_position_side(" short "));
        assert!(matches_short_position_side("SELL"));
        assert!(!matches_short_position_side("LONG"));
        assert_eq!(position_params(false, OrderSide::Sell), None);
        assert_eq!(
            position_params(true, OrderSide::Sell)
                .expect("params")
                .get("positionSide")
                .map(String::as_str),
            Some("SHORT")
        );
    }

    #[test]
    fn migrated_execution_helpers_should_size_order_from_precision() {
        let precision = SymbolPrecision {
            step_size: 0.01,
            tick_size: 0.001,
            min_notional: 5.0,
            min_order_size: 0.02,
        };

        assert_eq!(
            derive_order_quantity_from_precision(&precision, 10.0, 99.0),
            Some(0.1)
        );
        assert_eq!(
            derive_order_quantity_from_precision(&precision, 1.0, 100.0),
            Some(0.05)
        );
        assert_eq!(
            derive_order_quantity_from_precision(&precision, 0.0, 100.0),
            None
        );
    }

    #[test]
    fn migrated_execution_helpers_should_apply_maker_offsets_and_params() {
        let precision = SymbolPrecision {
            step_size: 0.001,
            tick_size: 0.01,
            min_notional: 5.0,
            min_order_size: 0.001,
        };

        assert!((maker_sell_price(&precision, 100.0, 1.0) - 100.01).abs() < 1e-9);
        assert!((maker_buy_price(&precision, 100.0, 1.0) - 99.99).abs() < 1e-9);
        let params =
            order_params_with_precision(true, OrderSide::Sell, &precision, 1.2300, Some(100.010));
        assert_eq!(
            params.get("positionSide").map(String::as_str),
            Some("SHORT")
        );
        assert_eq!(params.get("quantity").map(String::as_str), Some("1.23"));
        assert_eq!(params.get("price").map(String::as_str), Some("100.01"));
    }

    #[test]
    fn migrated_execution_helpers_should_match_pending_initial_order() {
        let pending = PendingInitialEntry {
            order_id: "12345".to_string(),
            client_order_id: Some("sll_enausdc_L1_1".to_string()),
            notional: 200.0,
            order_price: 1.0,
            reference_price: 1.0,
            submitted_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        };

        assert!(same_pending_initial_order(
            &ObservedOrder {
                order_id: "12345".to_string(),
                client_order_id: Some("other".to_string())
            },
            &pending
        ));
        assert!(same_pending_initial_order(
            &ObservedOrder {
                order_id: "99999".to_string(),
                client_order_id: Some("sll_enausdc_L1_1".to_string())
            },
            &pending
        ));
        assert!(!same_pending_initial_order(
            &ObservedOrder {
                order_id: "99999".to_string(),
                client_order_id: Some("other".to_string())
            },
            &pending
        ));
    }

    #[test]
    fn migrated_execution_helpers_should_filter_exit_orders_and_build_client_ids() {
        assert!(is_short_ladder_exit_order(&ObservedOrder {
            order_id: "1".to_string(),
            client_order_id: Some("sll_enausdc_close_123_00".to_string())
        }));
        assert!(is_short_ladder_exit_order(&ObservedOrder {
            order_id: "2".to_string(),
            client_order_id: Some("sll_enausdc_c_123_00".to_string())
        }));
        assert!(!is_short_ladder_exit_order(&ObservedOrder {
            order_id: "3".to_string(),
            client_order_id: Some("sll_enausdc_lm_123_00".to_string())
        }));

        let id = build_short_ladder_client_order_id_at(
            "VERY-LONG/ENAUSDC-PERP-SYMBOL",
            "close",
            103,
            1_765_432_123_456,
        );
        assert!(id.starts_with("sll_"));
        assert!(id.ends_with("_03"));
        assert!(id.len() <= 36);
    }

    #[test]
    fn migrated_execution_helpers_should_gate_initial_entry_fallback_and_layer_notionals() {
        assert!(should_fallback_initial_entry_to_market(true, Some(45), 0));
        assert!(!should_fallback_initial_entry_to_market(true, Some(45), 1));
        assert!(!should_fallback_initial_entry_to_market(false, Some(45), 0));
        assert!(!should_fallback_initial_entry_to_market(true, Some(0), 0));
        assert_eq!(
            layer_notionals_until(200.0, &[1.0, 1.0, 3.0, 5.0], 3),
            vec![200.0, 200.0, 600.0]
        );
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
            "webhook",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "{forbidden} should not appear in serialized runtime output"
            );
        }
    }

    fn sample_short_position() -> LiveShortPosition {
        let opened_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        LiveShortPosition {
            average_entry_price: 100.0,
            quantity: 4.0,
            current_notional: 400.0,
            filled_layers: 2,
            next_layer_index: 2,
            last_layer_price: 101.0,
            atr_at_entry: 2.0,
            breakeven_armed: false,
            trailing_take_profit_armed: false,
            best_favorable_price: None,
            opened_at,
            last_sync_at: opened_at,
        }
    }
}
