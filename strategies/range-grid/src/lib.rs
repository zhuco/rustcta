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
pub const MIGRATED_FROM: &str = "src/strategies/range_grid";

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
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
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
        }
    }

    fn snapshot_payload(&self) -> RangeGridSnapshotPayload {
        RangeGridSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
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
            "Partially migrated range grid strategy with adapter-free classifier and planner core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: Vec::new(),
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
                "remaining_legacy_modules".to_string(),
                json!([
                    "controller",
                    "tasks",
                    "notifications",
                    "risk_application",
                    "precision_service",
                    "indicator_service",
                    "websocket",
                    "runtime_execution"
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
        ExecutionOrderAck, ExecutionOrderCommand, OrderSide, SdkResult, StrategyExecutionClient,
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
