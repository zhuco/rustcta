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
    build_initial_grid_plan, price_levels, EngineAction, FeeConfig, FillEvent, FollowConfig,
    GridEngine, GridPlan, HedgedGridCoreConfig, MarketSnapshot, OrderDraft, OrderIntent,
    OrderLedger, OrderRecord, OrderSlot, PositionSide, PositionState, PriceReference,
    ResolvedPrecision, RiskReference, RiskState,
};

pub const STRATEGY_KIND: &str = "hedged_grid";
pub const DISPLAY_NAME: &str = "Hedged Grid";
pub const MIGRATED_FROM: &str = "src/strategies/hedged_grid";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HedgedGridStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for HedgedGridStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HedgedGridConfig {
    pub symbol: String,
    pub spot_exchange: String,
    pub hedge_exchange: String,
    pub grid_spacing_bps: f64,
    pub max_inventory_quote: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HedgedGridSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct HedgedGridRuntime {
    instance_id: StrategyInstanceId,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
}

impl HedgedGridRuntime {
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

    fn snapshot_payload(&self) -> HedgedGridSnapshotPayload {
        HedgedGridSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
        }
    }
}

impl Default for HedgedGridRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for HedgedGridRuntime {
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
            "Partially migrated hedged grid strategy with adapter-free grid planning core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: Vec::new(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places spot grid and hedge orders",
            ),
            risk_capability(RiskCapability::CancelOrders, "Cancels stale grid levels"),
            risk_capability(
                RiskCapability::Hedging,
                "Maintains offsetting hedge exposure",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Reserves inventory for grid levels",
            ),
        ],
        market_data_subscriptions: Vec::new(),
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
            ("partial_core_migration".to_string(), json!(true)),
            (
                "migrated_core_modules".to_string(),
                json!([
                    "core_config",
                    "follow_config",
                    "fee_config",
                    "precision_helpers",
                    "ledger",
                    "grid_side_book",
                    "engine_action_dto",
                    "fill_event_dto",
                    "grid_engine_rebuild",
                    "grid_engine_fill_roll",
                    "grid_engine_underwater_close",
                    "grid_engine_strict_pairing",
                    "grid_engine_pressure_budget",
                    "risk",
                    "initial_grid_planner"
                ]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!(["controller", "multi", "runtime_execution"]),
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
                "symbol",
                "spot_exchange",
                "hedge_exchange",
                "grid_spacing_bps",
                "max_inventory_quote"
            ],
            "properties": {
                "symbol": { "type": "string", "minLength": 1 },
                "spot_exchange": { "type": "string", "minLength": 1 },
                "hedge_exchange": { "type": "string", "minLength": 1 },
                "grid_spacing_bps": { "type": "number", "exclusiveMinimum": 0.0 },
                "max_inventory_quote": {
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
        let mut runtime = HedgedGridRuntime::new();
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
                "symbol": "BTC/USDT"
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
    fn migrated_core_should_build_symmetric_initial_grid() {
        let config = core_config_for_test();
        let snapshot = market_snapshot_for_test();
        let position = PositionState {
            equity: 1_000.0,
            maintenance_margin: 10.0,
            ..PositionState::default()
        };

        let plan = build_initial_grid_plan(&config, &snapshot, &position, 0.0)
            .expect("grid plan should build");

        assert_eq!(plan.symbol, "BTCUSDT");
        assert_eq!(plan.reference_price, 100.0);
        assert_eq!(plan.orders.len(), 4);
        assert_eq!(plan.orders[0].intent, OrderIntent::OpenLongBuy);
        assert_eq!(plan.orders[0].price, 99.0);
        assert_eq!(plan.orders[1].intent, OrderIntent::OpenLongBuy);
        assert_eq!(plan.orders[1].price, 98.0);
        assert_eq!(plan.orders[2].intent, OrderIntent::OpenShortSell);
        assert_eq!(plan.orders[2].price, 101.0);
        assert_eq!(plan.orders[3].intent, OrderIntent::OpenShortSell);
        assert_eq!(plan.orders[3].price, 102.0);
        assert!(plan
            .orders
            .iter()
            .all(|order| order.qty * order.price >= 10.0));
        assert!(plan.orders.iter().all(|order| order.post_only));
    }

    #[test]
    fn migrated_core_should_block_risky_opens() {
        let mut config = core_config_for_test();
        config.risk.max_total_notional = 10.0;
        let snapshot = market_snapshot_for_test();
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            equity: 1_000.0,
            maintenance_margin: 10.0,
            ..PositionState::default()
        };

        let plan = build_initial_grid_plan(&config, &snapshot, &position, 0.0)
            .expect("grid plan should build");

        assert!(plan.risk.flags.only_close);
        assert!(plan.orders.is_empty());
    }

    #[test]
    fn migrated_config_should_preserve_engine_default_flags() {
        let config: HedgedGridCoreConfig = serde_json::from_value(json!({
            "symbol": "SOLUSDC",
            "grid": {
                "levels_per_side": 3,
                "grid_spacing_pct": 0.001,
                "order_notional": 10.0
            },
            "precision": {
                "tick_size": 0.01,
                "step_size": 0.001
            },
            "risk": {
                "max_net_notional": 1000.0,
                "max_total_notional": 2000.0,
                "margin_ratio_limit": 0.8,
                "funding_rate_limit": 0.003,
                "funding_cost_limit": 5.0
            }
        }))
        .expect("config should deserialize with defaults");

        assert!(config.require_hedge_mode);
        assert!(config.grid.fill_remaining_slots_with_opens);
        assert!(config.grid.refill_open_slots_enabled);
        assert!(config.grid.normalize_open_grid_enabled);
        assert!(config.grid.follow_open_enabled);
        assert!(!config.grid.repair_near_gap_enabled);
        assert_eq!(config.follow.max_gap_steps, 1.0);
        assert_eq!(config.follow.follow_cooldown_ms, 800);
        assert_eq!(config.follow.max_follow_actions_per_minute, 30);
        assert_eq!(config.execution.cooldown_ms, 500);
        assert!(config.execution.post_only);
        assert_eq!(config.execution.post_only_retries, 3);
        assert_eq!(config.fees.maker_fee, 0.0);
        assert_eq!(config.fees.taker_fee, 0.0004);
    }

    #[test]
    fn migrated_precision_should_match_legacy_quantization_helpers() {
        let precision = ResolvedPrecision {
            tick_size: 0.01,
            step_size: 0.01,
            min_qty: 0.01,
            min_notional: 5.0,
            price_digits: 2,
            qty_digits: 2,
        };

        assert_eq!(precision.quantize_price(83.329), 83.32);
        assert_eq!(precision.quantize_qty(0.079), 0.07);
        assert_eq!(precision.quantize_qty_up(5.0 / 83.32), 0.07);
        assert_eq!(precision.quantize_qty_up(0.07), 0.07);
        assert_eq!(precision.quantize_qty_nearest(0.074), 0.07);
        assert_eq!(precision.quantize_qty_nearest(0.075), 0.08);
    }

    #[test]
    fn migrated_ledger_should_sort_update_and_remove_side_book_slots() {
        let mut buy_book = core::GridSideBook::new(rustcta_strategy_sdk::OrderSide::Buy);
        buy_book.insert(order_slot("buy-low", OrderIntent::OpenLongBuy, 98.0, 0.2));
        buy_book.insert(order_slot("buy-high", OrderIntent::OpenLongBuy, 99.0, 0.1));
        buy_book.insert(order_slot(
            "buy-close",
            OrderIntent::CloseShortBuy,
            97.0,
            0.3,
        ));

        assert_eq!(buy_book.nearest_price(), Some(99.0));
        assert_eq!(buy_book.farthest_price(), Some(97.0));
        assert_eq!(buy_book.count_by_intent(OrderIntent::OpenLongBuy), 2);
        assert!((buy_book.total_qty_for_intent(OrderIntent::OpenLongBuy) - 0.3).abs() < 1e-12);

        buy_book.update_qty("buy-low", 0.4);
        assert_eq!(
            buy_book
                .slots
                .iter()
                .find(|slot| slot.id == "buy-low")
                .map(|slot| slot.qty),
            Some(0.4)
        );
        assert_eq!(
            buy_book.remove("buy-high").map(|slot| slot.id),
            Some("buy-high".to_string())
        );
        assert_eq!(buy_book.nearest_price(), Some(98.0));

        let mut sell_book = core::GridSideBook::new(rustcta_strategy_sdk::OrderSide::Sell);
        sell_book.insert(order_slot(
            "sell-high",
            OrderIntent::OpenShortSell,
            102.0,
            0.1,
        ));
        sell_book.insert(order_slot(
            "sell-low",
            OrderIntent::OpenShortSell,
            101.0,
            0.2,
        ));

        assert_eq!(sell_book.nearest_price(), Some(101.0));
        assert_eq!(sell_book.farthest_price(), Some(102.0));
    }

    #[test]
    fn migrated_ledger_should_track_records_by_id() {
        let mut ledger = OrderLedger::new();
        ledger.insert(order_record("b", OrderIntent::OpenLongBuy, 99.0, 0.1));
        ledger.insert(order_record("a", OrderIntent::CloseLongSell, 101.0, 0.1));

        assert_eq!(ledger.all_ids(), vec!["a".to_string(), "b".to_string()]);
        assert_eq!(ledger.get("b").map(|record| record.price), Some(99.0));
        ledger.get_mut("b").expect("record should exist").retries = 2;
        assert_eq!(ledger.get("b").map(|record| record.retries), Some(2));

        let removed = ledger.remove("a").expect("record should be removed");
        assert_eq!(removed.intent, OrderIntent::CloseLongSell);
        assert!(ledger.get("a").is_none());
    }

    #[test]
    fn migrated_order_intent_should_expose_position_and_lifecycle() {
        assert_eq!(PositionSide::Long.as_str(), "LONG");
        assert_eq!(PositionSide::Short.as_str(), "SHORT");
        assert!(OrderIntent::OpenLongBuy.is_open());
        assert!(!OrderIntent::OpenLongBuy.is_close());
        assert!(OrderIntent::CloseShortBuy.is_close());
        assert_eq!(
            OrderIntent::CloseShortBuy.position_side(),
            PositionSide::Short
        );
    }

    #[test]
    fn migrated_engine_dtos_should_describe_actions_without_adapters() {
        let now = Utc::now();
        let position = PositionState {
            long_qty: 0.3,
            short_qty: 0.2,
            long_entry_price: 99.0,
            short_entry_price: 101.0,
            long_available: 0.25,
            short_available: 0.15,
            equity: 1_000.0,
            maintenance_margin: 10.0,
            mark_price: 100.0,
        };
        assert_eq!(position.long_available, 0.25);
        assert_eq!(position.short_entry_price, 101.0);

        let fill = FillEvent {
            order_id: "grid-1".to_string(),
            intent: OrderIntent::OpenLongBuy,
            fill_qty: 0.1,
            fill_price: 99.0,
            timestamp: now,
            partial: false,
        };
        assert_eq!(fill.intent.position_side(), PositionSide::Long);
        assert!(!fill.partial);

        let place = EngineAction::Place(OrderDraft {
            id: "grid-2".to_string(),
            intent: OrderIntent::CloseLongSell,
            price: 101.0,
            qty: 0.1,
            post_only: true,
        });
        let cancel = EngineAction::Cancel {
            order_id: "grid-3".to_string(),
            reason: "risk".to_string(),
        };

        assert!(matches!(place, EngineAction::Place(ref draft) if draft.intent.is_close()));
        assert!(matches!(cancel, EngineAction::Cancel { ref reason, .. } if reason == "risk"));
    }

    #[test]
    fn migrated_grid_engine_should_seed_open_and_close_ladders() {
        let levels = core_config_for_test().grid.levels_per_side;
        let mut engine =
            GridEngine::new(core_config_for_test(), true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        let snapshot = market_snapshot_for_test();

        let actions = engine.rebuild_grid(&snapshot);
        let (place_count, cancel_count) = action_counts(&actions);

        assert_eq!(place_count, levels * 4);
        assert_eq!(cancel_count, 0);
        assert_eq!(
            count_slots(&engine.buy_orders(), OrderIntent::OpenLongBuy),
            levels
        );
        assert_eq!(
            count_slots(&engine.buy_orders(), OrderIntent::CloseShortBuy),
            levels
        );
        assert_eq!(
            count_slots(&engine.sell_orders(), OrderIntent::OpenShortSell),
            levels
        );
        assert_eq!(
            count_slots(&engine.sell_orders(), OrderIntent::CloseLongSell),
            levels
        );
    }

    #[test]
    fn migrated_grid_engine_should_roll_after_open_long_fill() {
        let config = core_config_for_test();
        let levels = config.grid.levels_per_side;
        let spacing_abs = config.grid.grid_spacing_abs.expect("abs spacing");
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);
        let buy_order = engine
            .buy_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::OpenLongBuy)
            .expect("buy open order");

        let actions = engine.handle_fill(fill_for_slot(&buy_order, false), &snapshot);
        let (place_count, cancel_count) = action_counts(&actions);

        assert_eq!(place_count, 2);
        assert_eq!(cancel_count, 1);
        assert!(engine.order_record(&buy_order.id).is_none());
        assert_eq!(
            count_slots(&engine.buy_orders(), OrderIntent::OpenLongBuy),
            levels
        );
        assert_eq!(
            count_slots(&engine.sell_orders(), OrderIntent::CloseLongSell),
            levels
        );
        assert!(engine.buy_orders().iter().any(|order| {
            order.intent == OrderIntent::OpenLongBuy
                && (order.price - (buy_order.price - spacing_abs * levels as f64)).abs() < 1e-9
        }));
        assert!(engine.sell_orders().iter().any(|order| {
            order.intent == OrderIntent::CloseLongSell
                && (order.price - (buy_order.price + spacing_abs)).abs() < 1e-9
        }));
    }

    #[test]
    fn migrated_grid_engine_should_wait_for_complete_fill_before_rolling() {
        let mut engine =
            GridEngine::new(core_config_for_test(), true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);
        let buy_order = engine
            .buy_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::OpenLongBuy)
            .expect("buy open order");

        let mut partial = fill_for_slot(&buy_order, true);
        partial.fill_qty = 0.04;
        let actions = engine.handle_fill(partial, &snapshot);

        assert!(actions.is_empty());
        assert!(engine.order_record(&buy_order.id).is_some());

        let mut complete = fill_for_slot(&buy_order, false);
        complete.fill_qty = buy_order.qty - 0.04;
        let actions = engine.handle_fill(complete, &snapshot);
        let (place_count, cancel_count) = action_counts(&actions);

        assert_eq!(place_count, 2);
        assert_eq!(cancel_count, 1);
        assert!(engine.order_record(&buy_order.id).is_none());
    }

    #[test]
    fn migrated_grid_engine_should_roll_after_open_short_fill() {
        let config = core_config_for_test();
        let levels = config.grid.levels_per_side;
        let spacing_abs = config.grid.grid_spacing_abs.expect("abs spacing");
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);
        let sell_order = engine
            .sell_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::OpenShortSell)
            .expect("sell open order");

        let actions = engine.handle_fill(fill_for_slot(&sell_order, false), &snapshot);
        let (place_count, cancel_count) = action_counts(&actions);

        assert_eq!(place_count, 2);
        assert_eq!(cancel_count, 1);
        assert_eq!(
            count_slots(&engine.sell_orders(), OrderIntent::OpenShortSell),
            levels
        );
        assert_eq!(
            count_slots(&engine.buy_orders(), OrderIntent::CloseShortBuy),
            levels
        );
        assert!(engine.sell_orders().iter().any(|order| {
            order.intent == OrderIntent::OpenShortSell
                && (order.price - (sell_order.price + spacing_abs * levels as f64)).abs() < 1e-9
        }));
        assert!(engine.buy_orders().iter().any(|order| {
            order.intent == OrderIntent::CloseShortBuy
                && (order.price - (sell_order.price - spacing_abs)).abs() < 1e-9
        }));
    }

    #[test]
    fn migrated_grid_engine_should_reprice_or_cancel_post_only_rejects() {
        let mut config = core_config_for_test();
        config.execution.post_only_retries = 1;
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);
        let order = engine
            .buy_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::OpenLongBuy)
            .expect("open long order");

        let actions = engine.handle_post_only_reject(&order.id);
        let (place_count, cancel_count) = action_counts(&actions);
        assert_eq!(place_count, 1);
        assert_eq!(cancel_count, 1);
        let replacement = actions
            .iter()
            .find_map(|action| match action {
                EngineAction::Place(draft) => Some(draft),
                EngineAction::Cancel { .. } => None,
            })
            .expect("replacement place");
        assert!(replacement.price < order.price);

        let actions = engine.handle_post_only_reject(&replacement.id);
        let (place_count, cancel_count) = action_counts(&actions);
        assert_eq!(place_count, 0);
        assert_eq!(cancel_count, 1);
    }

    #[test]
    fn migrated_grid_engine_follow_should_shift_buy_ladder_when_inventory_is_short() {
        let mut config = core_config_for_test();
        config.follow.max_gap_steps = 0.5;
        config.follow.follow_cooldown_ms = 0;
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        engine.update_position(PositionState {
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
            ..PositionState::default()
        });
        engine.rebuild_grid(&market_snapshot_for_test());
        let before_min = engine.buy_orders().last().expect("buy min").price;
        let follow_snapshot = MarketSnapshot {
            best_bid: 119.0,
            best_ask: 121.0,
            last_price: 120.0,
            mark_price: 120.0,
            timestamp: Utc::now(),
        };

        let actions = engine.maybe_follow(&follow_snapshot);
        let after = engine.buy_orders();
        let after_min = after.last().expect("buy min").price;

        assert!(!actions.is_empty());
        assert_eq!(count_slots(&after, OrderIntent::OpenLongBuy), 2);
        assert!(after_min > before_min);
    }

    #[test]
    fn migrated_grid_engine_follow_should_not_shift_when_inventory_is_sufficient() {
        let mut config = core_config_for_test();
        config.follow.max_gap_steps = 0.5;
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        engine.rebuild_grid(&market_snapshot_for_test());
        let before = engine.buy_orders();
        let before_min = before.last().expect("buy min").price;
        let follow_snapshot = MarketSnapshot {
            best_bid: 119.0,
            best_ask: 121.0,
            last_price: 120.0,
            mark_price: 120.0,
            timestamp: Utc::now(),
        };

        let actions = engine.maybe_follow(&follow_snapshot);
        let after = engine.buy_orders();

        assert!(actions.is_empty());
        assert_eq!(after.len(), before.len());
        assert_eq!(after.last().expect("buy min").price, before_min);
    }

    #[test]
    fn migrated_grid_engine_reconcile_should_trim_excess_close_orders() {
        let mut engine =
            GridEngine::new(core_config_for_test(), true).expect("engine should build");
        engine.update_position(PositionState {
            long_qty: 0.4,
            long_available: 0.4,
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
            ..PositionState::default()
        });
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);

        engine.update_position(PositionState {
            long_qty: 0.1,
            long_available: 0.1,
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
            ..PositionState::default()
        });
        engine.reconcile_inventory(&snapshot);

        let close_orders = engine
            .sell_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::CloseLongSell)
            .collect::<Vec<_>>();
        let close_qty = close_orders.iter().map(|order| order.qty).sum::<f64>();

        assert!(close_orders.len() <= 1);
        assert!(close_qty <= 0.1 + 1e-6);
    }

    #[test]
    fn migrated_grid_engine_reconcile_should_not_create_duplicate_price_levels() {
        let mut engine =
            GridEngine::new(core_config_for_test(), true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);
        for _ in 0..5 {
            engine.reconcile_inventory(&snapshot);
        }

        assert_no_duplicate_intent_price(engine.buy_orders());
        assert_no_duplicate_intent_price(engine.sell_orders());
    }

    #[test]
    fn migrated_grid_engine_fill_should_repair_near_gap_and_trim_far_orders() {
        let mut config = core_config_for_test();
        config.grid.grid_spacing_abs = Some(2.5);
        config.grid.grid_spacing_pct = 0.0;
        config.grid.levels_per_side = 3;
        config.grid.order_qty = Some(0.011);
        config.grid.order_notional = 0.0;
        config.grid.repair_near_gap_enabled = true;
        config.precision.tick_size = 0.01;
        config.precision.price_digits = Some(2);
        config.precision.min_notional = Some(5.0);
        config.risk.max_net_notional = 100_000.0;
        config.risk.max_total_notional = 100_000.0;
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        engine.update_position(PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 2371.38,
            ..PositionState::default()
        });
        let snapshot = MarketSnapshot {
            best_bid: 2369.00862,
            best_ask: 2373.75138,
            last_price: 2371.38,
            mark_price: 2371.38,
            timestamp: Utc::now(),
        };
        engine.rebuild_grid(&snapshot);
        let near_sell = engine
            .sell_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::OpenShortSell)
            .min_by(|left, right| left.price.partial_cmp(&right.price).unwrap())
            .expect("near open short");

        let actions = engine.handle_fill(fill_for_slot(&near_sell, false), &snapshot);
        let (_, cancel_count) = action_counts(&actions);

        assert!(cancel_count >= 1, "far order should be trimmed");
        let highest_buy = engine
            .buy_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::OpenLongBuy)
            .map(|order| order.price)
            .reduce(f64::max)
            .expect("highest buy");
        let sell_open_prices = engine
            .sell_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::OpenShortSell)
            .map(|order| order.price)
            .collect::<Vec<_>>();
        let lowest_sell = sell_open_prices
            .iter()
            .copied()
            .reduce(f64::min)
            .expect("lowest sell");
        assert!(
            (lowest_sell - highest_buy - 5.0).abs() <= 0.01,
            "gap should be repaired to 2 * spacing_abs: buy={highest_buy} sell={lowest_sell}"
        );
        assert_eq!(sell_open_prices.len(), 3);
        let mut sorted_sell_prices = sell_open_prices;
        sorted_sell_prices.sort_by(|left, right| left.partial_cmp(right).unwrap());
        for pair in sorted_sell_prices.windows(2) {
            assert!(
                (pair[1] - pair[0] - 2.5).abs() <= 0.01,
                "sell ladder should remain continuous: {:?}",
                sorted_sell_prices
            );
        }
    }

    #[test]
    fn migrated_grid_engine_close_long_orders_should_follow_grid_when_underwater() {
        let mut engine =
            GridEngine::new(core_config_for_test(), true).expect("engine should build");
        engine.update_position(PositionState {
            long_qty: 1.0,
            long_entry_price: 100.0,
            long_available: 1.0,
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 90.0,
            ..PositionState::default()
        });
        let snapshot = MarketSnapshot {
            best_bid: 89.0,
            best_ask: 91.0,
            last_price: 90.0,
            mark_price: 90.0,
            timestamp: Utc::now(),
        };

        engine.rebuild_grid(&snapshot);
        let open_short_keys =
            price_keys_for_intent(engine.sell_orders(), OrderIntent::OpenShortSell);
        let close_long_orders = engine
            .sell_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::CloseLongSell)
            .collect::<Vec<_>>();

        assert!(!close_long_orders.is_empty());
        assert!(close_long_orders
            .iter()
            .all(|order| open_short_keys.contains(&test_price_key(order.price))));
        assert!(close_long_orders.iter().any(|order| order.price < 100.0));
    }

    #[test]
    fn migrated_grid_engine_close_short_orders_should_follow_grid_when_underwater() {
        let mut engine =
            GridEngine::new(core_config_for_test(), true).expect("engine should build");
        engine.update_position(PositionState {
            short_qty: 1.0,
            short_entry_price: 100.0,
            short_available: 1.0,
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 110.0,
            ..PositionState::default()
        });
        let snapshot = MarketSnapshot {
            best_bid: 109.0,
            best_ask: 111.0,
            last_price: 110.0,
            mark_price: 110.0,
            timestamp: Utc::now(),
        };

        engine.rebuild_grid(&snapshot);
        let open_long_keys = price_keys_for_intent(engine.buy_orders(), OrderIntent::OpenLongBuy);
        let close_short_orders = engine
            .buy_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::CloseShortBuy)
            .collect::<Vec<_>>();

        assert!(!close_short_orders.is_empty());
        assert!(close_short_orders
            .iter()
            .all(|order| open_long_keys.contains(&test_price_key(order.price))));
        assert!(close_short_orders.iter().any(|order| order.price > 100.0));
    }

    #[test]
    fn migrated_grid_engine_strict_pairing_should_not_force_close_price_to_entry() {
        let mut config = core_config_for_test();
        config.grid.strict_pairing = true;
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        engine.update_position(PositionState {
            long_entry_price: 100.0,
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 99.0,
            ..PositionState::default()
        });
        let snapshot = MarketSnapshot {
            best_bid: 98.0,
            best_ask: 100.0,
            last_price: 99.0,
            mark_price: 99.0,
            timestamp: Utc::now(),
        };
        engine.rebuild_grid(&snapshot);
        let open_long = engine
            .buy_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::OpenLongBuy)
            .expect("strict open long");

        engine.handle_fill(fill_for_slot(&open_long, false), &snapshot);
        let close_long = engine
            .sell_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::CloseLongSell)
            .expect("paired close long");

        assert!(
            close_long.price < 100.0,
            "legacy strict pairing must not push close price back to entry"
        );
    }

    #[test]
    fn migrated_grid_engine_same_price_double_sell_fills_should_roll_twice() {
        let levels = core_config_for_test().grid.levels_per_side;
        let mut engine =
            GridEngine::new(core_config_for_test(), true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);
        let open_short = engine
            .sell_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::OpenShortSell)
            .expect("open short");
        let close_long = engine
            .sell_orders()
            .into_iter()
            .find(|order| {
                order.intent == OrderIntent::CloseLongSell
                    && test_price_key(order.price) == test_price_key(open_short.price)
            })
            .expect("same-price close long");

        let open_actions = engine.handle_fill(fill_for_slot(&open_short, false), &snapshot);
        let close_actions = engine.handle_fill(fill_for_slot(&close_long, false), &snapshot);
        let (open_places, open_cancels) = action_counts(&open_actions);
        let (close_places, close_cancels) = action_counts(&close_actions);

        assert_eq!(open_places, 2);
        assert_eq!(open_cancels, 1);
        assert_eq!(close_places, 2);
        assert!(
            close_cancels >= 1,
            "close fill should roll even when shape repair trims extra far orders"
        );
        assert_eq!(engine.order_ids().len(), levels * 4);
        assert_eq!(
            count_slots(&engine.sell_orders(), OrderIntent::OpenShortSell),
            levels
        );
        assert_eq!(
            count_slots(&engine.sell_orders(), OrderIntent::CloseLongSell),
            levels
        );
    }

    #[test]
    fn migrated_grid_engine_strict_close_fill_should_not_create_unpaired_close() {
        let mut config = core_config_for_test();
        config.grid.strict_pairing = true;
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        engine.update_position(PositionState {
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
            ..PositionState::default()
        });
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);
        let open_long = engine
            .buy_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::OpenLongBuy)
            .expect("strict open long");
        engine.handle_fill(fill_for_slot(&open_long, false), &snapshot);
        let close_long = engine
            .sell_orders()
            .into_iter()
            .find(|order| order.intent == OrderIntent::CloseLongSell)
            .expect("paired close long");

        engine.handle_fill(fill_for_slot(&close_long, false), &snapshot);

        assert_eq!(
            count_slots(&engine.sell_orders(), OrderIntent::CloseLongSell),
            0
        );
    }

    #[test]
    fn migrated_grid_engine_pending_open_orders_should_count_against_total_notional() {
        let mut config = core_config_for_test();
        config.grid.strict_pairing = true;
        config.grid.levels_per_side = 4;
        config.risk.max_total_notional = 25.0;
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        let snapshot = market_snapshot_for_test();

        engine.update_position(PositionState {
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
            ..PositionState::default()
        });
        engine.rebuild_grid(&snapshot);

        assert!(engine.order_ids().len() < 8);
        assert!(
            current_and_pending_open_notional_for_test(&engine, 100.0) <= 25.0 + 1e-9,
            "pending opens must consume the max_total_notional budget"
        );
    }

    #[test]
    fn migrated_grid_engine_strict_repeated_open_fills_should_stop_at_total_notional_limit() {
        let mut config = core_config_for_test();
        config.grid.strict_pairing = true;
        config.grid.levels_per_side = 4;
        config.risk.max_total_notional = 35.0;
        let mut engine = GridEngine::new(config, true).expect("engine should build");
        let snapshot = market_snapshot_for_test();
        engine.update_position(PositionState {
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
            ..PositionState::default()
        });
        engine.rebuild_grid(&snapshot);

        for _ in 0..6 {
            let Some(open_order) = engine
                .buy_orders()
                .into_iter()
                .chain(engine.sell_orders())
                .find(|order| order.intent.is_open())
            else {
                break;
            };
            engine.handle_fill(fill_for_slot(&open_order, false), &snapshot);
            engine.reconcile_inventory(&snapshot);
            assert!(
                current_and_pending_open_notional_for_test(&engine, 100.0) <= 35.0 + 1e-9,
                "position plus pending opens should stay within the total budget"
            );
        }
    }

    #[test]
    fn migrated_grid_engine_kill_switch_should_cancel_all_orders() {
        let mut engine =
            GridEngine::new(core_config_for_test(), true).expect("engine should build");
        engine.update_position(balanced_position_for_engine());
        let snapshot = market_snapshot_for_test();
        engine.rebuild_grid(&snapshot);
        assert!(!engine.order_ids().is_empty());

        let actions = engine.trigger_kill_switch("manual");
        let (place_count, cancel_count) = action_counts(&actions);

        assert_eq!(place_count, 0);
        assert!(cancel_count > 0);
        assert!(engine.order_ids().is_empty());
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

    fn order_slot(id: &str, intent: OrderIntent, price: f64, qty: f64) -> OrderSlot {
        OrderSlot {
            id: id.to_string(),
            intent,
            price,
            qty,
        }
    }

    fn order_record(id: &str, intent: OrderIntent, price: f64, qty: f64) -> OrderRecord {
        OrderRecord {
            id: id.to_string(),
            intent,
            price,
            qty,
            filled_qty: 0.0,
            created_at: Utc::now(),
            retries: 0,
        }
    }

    fn count_slots(slots: &[OrderSlot], intent: OrderIntent) -> usize {
        slots.iter().filter(|slot| slot.intent == intent).count()
    }

    fn action_counts(actions: &[EngineAction]) -> (usize, usize) {
        let place = actions
            .iter()
            .filter(|action| matches!(action, EngineAction::Place(_)))
            .count();
        let cancel = actions
            .iter()
            .filter(|action| matches!(action, EngineAction::Cancel { .. }))
            .count();
        (place, cancel)
    }

    fn fill_for_slot(slot: &OrderSlot, partial: bool) -> FillEvent {
        FillEvent {
            order_id: slot.id.clone(),
            intent: slot.intent,
            fill_qty: slot.qty,
            fill_price: slot.price,
            timestamp: Utc::now(),
            partial,
        }
    }

    fn assert_no_duplicate_intent_price(slots: Vec<OrderSlot>) {
        let mut seen = std::collections::HashSet::new();
        for slot in slots {
            let key = (slot.intent as u8, (slot.price * 10_000.0).round() as i64);
            assert!(seen.insert(key), "duplicate intent/price level: {:?}", slot);
        }
    }

    fn price_keys_for_intent(
        slots: Vec<OrderSlot>,
        intent: OrderIntent,
    ) -> std::collections::HashSet<i64> {
        slots
            .into_iter()
            .filter(|slot| slot.intent == intent)
            .map(|slot| test_price_key(slot.price))
            .collect()
    }

    fn test_price_key(price: f64) -> i64 {
        (price * 10.0).round() as i64
    }

    fn current_and_pending_open_notional_for_test(engine: &GridEngine, mark_price: f64) -> f64 {
        engine
            .order_ids()
            .into_iter()
            .filter_map(|id| engine.order_record(&id))
            .filter(|record| record.intent.is_open())
            .map(|record| record.price.abs().max(mark_price) * record.qty.abs())
            .sum::<f64>()
    }

    fn balanced_position_for_engine() -> PositionState {
        PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10_000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        }
    }

    fn core_config_for_test() -> HedgedGridCoreConfig {
        HedgedGridCoreConfig {
            symbol: "BTCUSDT".to_string(),
            require_hedge_mode: true,
            price_reference: PriceReference::Mid,
            risk_reference: RiskReference::Mark,
            grid: core::GridConfig {
                levels_per_side: 2,
                grid_spacing_pct: 0.0,
                grid_spacing_abs: Some(1.0),
                order_notional: 10.0,
                order_qty: None,
                fill_remaining_slots_with_opens: true,
                strict_pairing: false,
                refill_open_slots_enabled: true,
                normalize_open_grid_enabled: true,
                follow_open_enabled: true,
                repair_near_gap_enabled: false,
            },
            follow: FollowConfig {
                max_gap_steps: 1.0,
                follow_cooldown_ms: 0,
                max_follow_actions_per_minute: 100,
            },
            execution: core::ExecutionConfig {
                cooldown_ms: 0,
                post_only: true,
                post_only_retries: 3,
            },
            precision: core::PrecisionConfig {
                tick_size: 0.1,
                step_size: 0.001,
                min_qty: Some(0.001),
                min_notional: Some(10.0),
                price_digits: Some(1),
                qty_digits: Some(3),
            },
            fees: FeeConfig {
                maker_fee: 0.0,
                taker_fee: 0.0004,
            },
            risk: core::RiskLimits {
                max_net_notional: 1_000.0,
                max_total_notional: 1_000.0,
                margin_ratio_limit: 0.5,
                funding_rate_limit: 1.0,
                funding_cost_limit: 1_000.0,
            },
        }
    }

    fn market_snapshot_for_test() -> MarketSnapshot {
        MarketSnapshot {
            best_bid: 99.0,
            best_ask: 101.0,
            last_price: 100.5,
            mark_price: 100.0,
            timestamp: Utc::now(),
        }
    }
}
