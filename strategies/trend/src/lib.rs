use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, RequiredAccountPermission, RiskCapability, RiskCapabilityDeclaration,
    StrategyConfigSchema, StrategyContext, StrategyEvent, StrategyInstanceId, StrategyRuntime,
    StrategySnapshot, StrategySnapshotSchema, StrategySpec, StrategyStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub mod core;

pub use core::{
    allocate_entry_notional, apply_entry_price_improve, approve_trade, bollinger_bands,
    build_entry_orders, build_performance_report, build_trend_entry_client_id,
    build_trend_execution_order_plan, calculate_initial_stop, calculate_kelly_factor,
    calculate_monitor_sharpe_ratio, calculate_profit_r, calculate_stop_loss, calculate_stop_update,
    calculate_take_profits, calculate_trailing_stop, check_partial_profits, classify_regime,
    compute_adx, compute_atr, compute_bollinger, compute_data_quality, compute_ema,
    compute_execution_price, compute_execution_slippage, compute_fib_anchors,
    compute_indicator_outputs, compute_ofi, compute_orderbook_tilt, compute_rsi,
    compute_slope_metric, compute_trend_snapshot, current_stop_type, current_utc_hour,
    emergency_stop, evaluate_account_layer, evaluate_order_layer, evaluate_pnl_trailing,
    evaluate_risk_layers, evaluate_strategy_layer, evaluate_system_layer,
    execution_decimals_from_step, find_breakout_entry, find_pullback_entry,
    format_execution_decimal, generate_trade_signal, get_stop_report, handle_indicator_event_core,
    has_minimum_history, is_at_breakeven, planned_notional, price_from_bollinger, price_from_fib,
    price_from_moving_average, quantize_execution_amount, quantize_execution_maker_price,
    quantize_execution_price, record_monitor_trade, refresh_monitor_statistics,
    requires_one_hour_history, should_update_stop, side_label, time_factor, trend_factor,
    update_monitor_drawdown, validate_signal_quality, volatility_factor, AccountCapital,
    AccountMetrics, AllocationRegimesConfig, AllocationResult, BollingerSnapshot, CandleBook,
    CandleEvent, CandleInterval, CandleSnapshot, EntryAllocation, EntryAllocationSlice, EntryMode,
    EntryOrderPlan, ExecutionConfig, IndicatorComputationError, IndicatorConfig,
    IndicatorEventDecision, IndicatorEventSkipReason, IndicatorSnapshot, MarketDataConfig,
    MarketRegime, MonitorTradeRecord, MonitoringConfig, OrderMetrics, PartialTakeProfit,
    PartialTarget, PerformanceMetrics, PerformanceReport, PnlLockLevel, PnlTrailingConfig,
    PositionBook, PositionConfig, PositionSizingError, PyramidEntry, PyramidLevel, PyramidSignal,
    RegimeConfig, RiskBudgetConfig, RiskConfig, RiskLevel, RiskSnapshot, ScoringConfig,
    SharedTrendData, SignalConfig, SignalMetadata, SignalType, StatusReportConfig, StopConfig,
    StopReport, StopType, StopUpdate, StopUpdateInputs, StrategyMetrics, SymbolPrecision,
    SystemHealth, TakeProfit, TakeProfitLevel, TimeCurveConfig, TradeSignal, TrendCandle,
    TrendConfig, TrendConfigError, TrendDirection, TrendExecutionOrderPlan, TrendFibAnchors,
    TrendIndicatorOutputs, TrendIndicatorSnapshot, TrendPosition, TrendSignal,
};

pub const STRATEGY_KIND: &str = "trend";
pub const DISPLAY_NAME: &str = "Trend";
pub const MIGRATED_FROM: &str = "src/strategies/trend";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrendStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for TrendStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendRuntimeConfig {
    pub symbols: Vec<String>,
    pub entry_leverage: String,
    pub max_positions: usize,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrendSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct TrendRuntime {
    instance_id: StrategyInstanceId,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
}

impl TrendRuntime {
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

    fn snapshot_payload(&self) -> TrendSnapshotPayload {
        TrendSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
        }
    }
}

impl Default for TrendRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for TrendRuntime {
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
            "Partially migrated intraday trend strategy with adapter-free core.".to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: Vec::new(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places trend entry and exit orders",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels managed bracket orders",
            ),
            risk_capability(
                RiskCapability::ReduceOnlyOrders,
                "Reduces or closes trend positions through bracket exits",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Controls symbol and notional inventory limits",
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
                    "config_validation",
                    "trend_signal_dto",
                    "trade_signal_generation",
                    "entry_allocation",
                    "entry_order_planning",
                    "position_book",
                    "position_sizing",
                    "pyramid_rules",
                    "pnl_accounting",
                    "candle_interval_dto",
                    "candle_book_state",
                    "indicator_helpers",
                    "data_quality_gate",
                    "indicator_output_composition",
                    "trend_snapshot_composition",
                    "indicator_event_decision",
                    "monitoring_metrics",
                    "execution_order_planning",
                    "stop_state_machine",
                    "partial_take_profit",
                    "risk_layer_dto",
                    "risk_layer_evaluation",
                    "trade_approval"
                ]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!([
                    "strategy_runtime",
                    "shared_data_ingestion",
                    "indicator_service",
                    "market_feed_runtime",
                    "execution_engine",
                    "order_tracker",
                    "stop_manager",
                    "position_sync",
                    "user_stream",
                    "monitoring",
                    "webhook_status_reports",
                    "account_manager_wiring"
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
            "required": ["symbols", "entry_leverage", "max_positions"],
            "properties": {
                "symbols": {
                    "type": "array",
                    "items": { "type": "string", "minLength": 1 },
                    "minItems": 1
                },
                "entry_leverage": {
                    "type": "string",
                    "pattern": "^[0-9]+(\\.[0-9]+)?$"
                },
                "max_positions": { "type": "integer", "minimum": 1 },
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
                "started_at": { "type": ["string", "null"], "format": "date-time" },
                "last_event_at": { "type": ["string", "null"], "format": "date-time" }
            }
        }),
    }
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
        ExecutionOrderAck, ExecutionOrderCommand, OrderSide, OrderType, SdkResult,
        StrategyExecutionClient, TimeInForce,
    };
    use serde_json::Value;
    use std::collections::HashMap;
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
        assert_eq!(spec.snapshot_schema.schema_version, 1);
        assert_eq!(spec.snapshot_schema.json_schema["type"], json!("object"));
        assert_secret_free(&serde_json::to_value(spec).expect("spec serializes"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_should_be_secret_free() {
        let mut runtime = TrendRuntime::new();
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
                "symbols": ["BTC/USDC"]
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        let snapshot = runtime.snapshot().await.expect("snapshot should build");

        assert_eq!(snapshot.strategy_kind, STRATEGY_KIND);
        assert_eq!(snapshot.status, StrategyStatus::Running);
        assert_secret_free(&snapshot.payload);
        assert_secret_free(&serde_json::to_value(snapshot).expect("snapshot serializes"));
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
    fn migrated_config_should_validate_core_limits() {
        let mut config = base_config();
        config.validate_core().expect("valid config");
        config.risk_config.max_leverage = 6.0;
        assert_eq!(config.validate_core(), Err(TrendConfigError::MaxLeverage));
    }

    #[test]
    fn trend_signal_tradeable_should_not_require_timeframe_alignment() {
        let signal = TrendSignal {
            direction: TrendDirection::Bullish,
            strength: 70.0,
            confidence: 61.0,
            timeframe_aligned: false,
            volume_confirmation: true,
            data_quality_score: 50.0,
        };

        assert!(signal.is_tradeable());
    }

    #[test]
    fn signal_generation_should_build_breakout_and_pullback_signals() {
        let now = Utc::now();
        let config = base_config();
        let indicators = IndicatorSnapshot {
            last_price: 100.0,
            atr: 2.0,
            bollinger_middle: 99.0,
        };

        let strong = generate_trade_signal(
            &config.signal_config,
            "BTC/USDC",
            &TrendSignal {
                direction: TrendDirection::StrongBullish,
                strength: 80.0,
                confidence: 75.0,
                timeframe_aligned: true,
                volume_confirmation: true,
                data_quality_score: 100.0,
            },
            &indicators,
            now,
        )
        .expect("strong trend signal");
        let pullback = generate_trade_signal(
            &config.signal_config,
            "BTC/USDC",
            &TrendSignal {
                direction: TrendDirection::Bearish,
                strength: 70.0,
                confidence: 80.0,
                timeframe_aligned: true,
                volume_confirmation: true,
                data_quality_score: 100.0,
            },
            &indicators,
            now,
        )
        .expect("pullback signal");

        assert_eq!(strong.signal_type, SignalType::TrendBreakout);
        assert_eq!(strong.side, OrderSide::Buy);
        assert!(strong.entry_price > indicators.last_price);
        assert_eq!(pullback.signal_type, SignalType::Pullback);
        assert_eq!(pullback.side, OrderSide::Sell);
        assert!(pullback.entry_price > indicators.last_price);
    }

    #[test]
    fn signal_quality_should_reject_wide_stop_or_low_confidence() {
        let mut signal = sample_signal("BTC/USDC", OrderSide::Buy, 100.0, 94.0);
        assert!(!validate_signal_quality(&signal));
        signal.stop_loss = 98.0;
        signal.confidence = 50.0;
        assert!(!validate_signal_quality(&signal));
        signal.confidence = 80.0;
        assert!(validate_signal_quality(&signal));
    }

    #[test]
    fn entry_allocation_should_concentrate_when_below_min_notional() {
        let config = base_config();
        let result = allocate_entry_notional(
            &config.entry_allocation,
            &config.allocation_regimes,
            MarketRegime::Trending,
            10.0,
            6.0,
        );

        assert_eq!(result.per_mode.len(), 1);
        assert_eq!(result.per_mode[0].mode, EntryMode::MovingAverage);
        assert_eq!(result.per_mode[0].notional, 10.0);
    }

    #[test]
    fn entry_order_plan_should_build_three_adapter_free_orders() {
        let config = base_config();
        let signal = sample_signal("ETH/USDC", OrderSide::Buy, 100.0, 98.0);
        let allocation = allocate_entry_notional(
            &config.entry_allocation,
            &config.allocation_regimes,
            MarketRegime::Trending,
            90.0,
            20.0,
        );
        let shared = shared_data();

        let orders = build_entry_orders(&signal, &shared, &allocation, 2.0);

        assert_eq!(orders.len(), 3);
        assert!(orders.iter().all(|order| order.notional >= 20.0));
        assert!(orders.iter().any(|order| {
            order.mode == EntryMode::Fibonacci && (order.price - 97.64 * 0.9998).abs() < 1e-9
        }));
    }

    #[test]
    fn position_book_should_size_and_track_dual_positions() {
        let config = base_config();
        let mut book = PositionBook::new(
            config.position_config.clone(),
            HashMap::new(),
            HashMap::new(),
            config.risk_config.max_leverage,
        );
        let long = sample_signal("BTC/USDC", OrderSide::Buy, 100.0, 98.0);
        let short = sample_signal("BTC/USDC", OrderSide::Sell, 100.0, 102.0);

        let size = book
            .calculate_position_size(
                &long,
                AccountCapital {
                    total: 1000.0,
                    available: 1000.0,
                },
                Some(50.0),
                12,
            )
            .expect("size");
        book.add_position(long, size, Utc::now());
        book.add_position(short, 0.4, Utc::now());

        assert_eq!(size, 0.5);
        assert!(book.has_open_position_side("BTC/USDC", OrderSide::Buy));
        assert!(book.has_open_position_side("BTC/USDC", OrderSide::Sell));
        assert_eq!(book.total_open_positions(), 2);
    }

    #[test]
    fn position_book_should_pyramid_update_pnl_and_close_by_side() {
        let mut config = base_config();
        config.position_config.pyramid_enabled = true;
        config.position_config.pyramid_levels = vec![PyramidLevel {
            trigger_profit: 1.0,
            size_ratio: 0.5,
        }];
        let mut book = PositionBook::new(
            config.position_config.clone(),
            HashMap::new(),
            HashMap::new(),
            3.0,
        );
        let now = Utc::now();
        let signal = sample_signal("SOL/USDC", OrderSide::Buy, 100.0, 95.0);
        let position = book.add_position(signal, 1.0, now);

        let pyramid = book
            .should_pyramid(&position, 106.0)
            .expect("pyramid should trigger");
        book.pyramid_position("SOL/USDC", OrderSide::Buy, pyramid, now)
            .expect("pyramid applies");
        book.update_position_pnl("SOL/USDC", OrderSide::Buy, 110.0, now)
            .expect("pnl updates");
        let closed = book
            .close_position_with_fill("SOL/USDC", OrderSide::Buy, 110.0, now)
            .expect("position closes");

        assert_eq!(closed.current_size, 0.0);
        assert!(closed.realized_pnl > 0.0);
        assert!(!book.has_open_position_side("SOL/USDC", OrderSide::Buy));
    }

    #[test]
    fn risk_layers_and_trade_approval_should_match_legacy_thresholds() {
        let config = base_config();
        let mut snapshot = risk_snapshot();
        assert_eq!(
            evaluate_risk_layers(&config.risk_config, &snapshot),
            RiskLevel::Normal
        );
        let signal = sample_signal("BTC/USDC", OrderSide::Buy, 100.0, 99.0);
        assert!(approve_trade(&config.risk_config, &snapshot, &signal, 5.0));

        snapshot.account.daily_pnl = -60.0;
        assert_eq!(
            evaluate_risk_layers(&config.risk_config, &snapshot),
            RiskLevel::Danger
        );
        assert!(!approve_trade(&config.risk_config, &snapshot, &signal, 5.0));
    }

    #[test]
    fn stop_core_should_calculate_initial_stops_from_atr_or_fixed_distance() {
        let config = base_config();
        let stop = &config.stop_config;

        let long_stop = calculate_initial_stop(stop, 100.0, OrderSide::Buy, Some(2.0));
        let short_stop = calculate_initial_stop(stop, 100.0, OrderSide::Sell, Some(2.0));

        assert_eq!(long_stop, 96.0);
        assert_eq!(short_stop, 104.0);

        let mut fixed = stop.clone();
        fixed.initial_stop_type = StopType::Fixed;
        assert_eq!(
            calculate_initial_stop(&fixed, 100.0, OrderSide::Buy, None),
            98.0
        );
    }

    #[test]
    fn stop_core_should_lock_profit_before_other_trailing_rules() {
        let config = base_config();
        let position = sample_position("BTC/USDC", OrderSide::Buy, 100.0, 95.0, 1.0);
        let update = calculate_stop_update(
            &config.stop_config,
            &position,
            &StopUpdateInputs {
                current_price: 101.0,
                atr_fast: 2.0,
                atr_slow: 1.5,
                now: position.entry_time + chrono::Duration::hours(1),
            },
        );

        assert_eq!(update, Some(StopUpdate::MoveTo(100.1)));
    }

    #[test]
    fn stop_core_should_time_stop_only_losing_old_positions() {
        let mut config = base_config();
        config.stop_config.lock_profit_pct = 0.5;
        config.stop_config.breakeven_enabled = false;
        config.stop_config.pnl_trailing.enable = false;
        config.stop_config.trailing_stop_enabled = false;
        let position = sample_position("ETH/USDC", OrderSide::Buy, 100.0, 95.0, 1.0);

        let update = calculate_stop_update(
            &config.stop_config,
            &position,
            &StopUpdateInputs {
                current_price: 99.0,
                atr_fast: 2.0,
                atr_slow: 2.0,
                now: position.entry_time + chrono::Duration::hours(25),
            },
        );

        assert_eq!(update, Some(StopUpdate::MoveTo(99.0)));
    }

    #[test]
    fn stop_core_should_emit_breakeven_and_pnl_trailing_updates() {
        let mut config = base_config();
        config.stop_config.lock_profit_pct = 0.5;
        config.stop_config.pnl_trailing.enable = false;
        let position = sample_position("SOL/USDC", OrderSide::Buy, 100.0, 95.0, 1.0);

        let breakeven = calculate_stop_update(
            &config.stop_config,
            &position,
            &StopUpdateInputs {
                current_price: 103.0,
                atr_fast: 2.0,
                atr_slow: 2.0,
                now: position.entry_time + chrono::Duration::hours(1),
            },
        );

        assert_eq!(breakeven, Some(StopUpdate::Breakeven));

        config.stop_config.breakeven_enabled = false;
        config.stop_config.pnl_trailing.enable = true;
        config.stop_config.pnl_trailing.lock_levels = vec![PnlLockLevel {
            profit_atr: 2.0,
            stop_offset_atr: 0.5,
        }];
        let pnl = calculate_stop_update(
            &config.stop_config,
            &position,
            &StopUpdateInputs {
                current_price: 105.0,
                atr_fast: 2.0,
                atr_slow: 2.0,
                now: position.entry_time + chrono::Duration::hours(1),
            },
        );

        assert_eq!(pnl, Some(StopUpdate::MoveTo(104.0)));
    }

    #[test]
    fn stop_core_should_detect_partial_take_profit_and_report_state() {
        let config = base_config();
        let position = sample_position("BTC/USDC", OrderSide::Buy, 100.0, 95.0, 2.0);

        let partials = check_partial_profits(&config.stop_config, &position, 106.0);
        let report = get_stop_report(&config.stop_config, &position);

        assert_eq!(partials.len(), 1);
        assert_eq!(partials[0].index, 0);
        assert_eq!(partials[0].size, 0.6);
        assert_eq!(report.stop_type, "initial");
        assert_eq!(report.current_stop, 95.0);
        assert!(!report.is_breakeven);
        assert_eq!(emergency_stop(&position), 0.0);
    }

    #[test]
    fn candle_interval_should_parse_supported_labels() {
        assert_eq!(
            CandleInterval::from_label("1m"),
            Some(CandleInterval::OneMinute)
        );
        assert_eq!(
            CandleInterval::from_label("5m"),
            Some(CandleInterval::FiveMinutes)
        );
        assert_eq!(CandleInterval::OneHour.label(), "1h");
        assert_eq!(CandleInterval::from_label("2h"), None);
    }

    #[test]
    fn candle_book_should_replace_same_open_time() {
        let mut book = CandleBook::new(3);
        let first = sample_candle(0, 100.0);
        let replacement = sample_candle(0, 101.0);

        assert!(book.apply_event(sample_candle_event("BTC/USDC", first)));
        assert!(book.apply_event(sample_candle_event("BTC/USDC", replacement)));

        let snapshot = book.snapshot("BTC/USDC").expect("snapshot exists");
        assert_eq!(snapshot.len(CandleInterval::OneMinute), 1);
        assert_eq!(snapshot.one_minute[0].close, 101.0);
    }

    #[test]
    fn candle_book_should_append_newer_and_trim_capacity() {
        let mut book = CandleBook::new(2);

        assert!(book.apply_event(sample_candle_event("ETH/USDC", sample_candle(0, 100.0))));
        assert!(book.apply_event(sample_candle_event("ETH/USDC", sample_candle(1, 101.0))));
        assert!(book.apply_event(sample_candle_event("ETH/USDC", sample_candle(2, 102.0))));

        let snapshot = book.snapshot("ETH/USDC").expect("snapshot exists");
        assert_eq!(snapshot.one_minute.len(), 2);
        assert_eq!(snapshot.one_minute[0].close, 101.0);
        assert_eq!(snapshot.one_minute[1].close, 102.0);
    }

    #[test]
    fn candle_book_should_ignore_older_open_time() {
        let mut book = CandleBook::new(3);

        assert!(book.apply_event(sample_candle_event("SOL/USDC", sample_candle(2, 102.0))));
        assert!(!book.apply_event(sample_candle_event("SOL/USDC", sample_candle(1, 101.0))));

        let snapshot = book.snapshot("SOL/USDC").expect("snapshot exists");
        assert_eq!(snapshot.one_minute.len(), 1);
        assert_eq!(snapshot.one_minute[0].close, 102.0);
    }

    #[test]
    fn candle_book_seed_should_sort_keep_latest_and_emit_final_event() {
        let mut book = CandleBook::new(2);

        let event = book
            .seed(
                "BTC/USDC",
                CandleInterval::FiveMinutes,
                vec![
                    sample_candle(2, 102.0),
                    sample_candle(0, 100.0),
                    sample_candle(1, 101.0),
                ],
            )
            .expect("seed emits last event");

        assert_eq!(event.symbol, "BTC/USDC");
        assert_eq!(event.interval, CandleInterval::FiveMinutes);
        assert!(event.is_final);
        assert_eq!(event.candle.close, 102.0);

        let snapshot = book.snapshot("BTC/USDC").expect("snapshot exists");
        assert_eq!(snapshot.len(CandleInterval::FiveMinutes), 2);
        assert_eq!(snapshot.five_minutes[0].close, 101.0);
        assert_eq!(snapshot.five_minutes[1].close, 102.0);
        assert!(snapshot.one_minute.is_empty());
    }

    #[test]
    fn indicator_helpers_should_compute_bollinger_fallback_error_and_clamps() {
        let fallback = compute_bollinger(&[1.0, 2.0, 3.0, 4.0], 20, 2.0, 99.0)
            .expect("fallback should not fail");
        assert_eq!(fallback.upper, 99.0);
        assert_eq!(fallback.middle, 99.0);
        assert_eq!(fallback.lower, 99.0);
        assert_eq!(fallback.sigma, 0.0);
        assert_eq!(fallback.band_percent, 0.5);
        assert_eq!(fallback.z_score, 0.0);

        let err = compute_bollinger(&[1.0, 2.0, 3.0, 4.0, 5.0], 20, 2.0, 0.0)
            .expect_err("len >= 5 but len < period should fail");
        assert_eq!(err, IndicatorComputationError::BollingerCalculationFailed);

        let flat = compute_bollinger(&[10.0; 20], 20, 2.0, 10.0).expect("flat bands compute");
        assert_eq!(flat.sigma, 0.0);
        assert_eq!(flat.band_percent, 0.0);
        assert_eq!(flat.z_score, 0.0);

        let high_last =
            compute_bollinger(&[1.0, 1.0, 1.0, 1.0, 100.0], 5, 2.0, 0.0).expect("computes");
        assert_eq!(high_last.band_percent, 1.0);

        let low_last =
            compute_bollinger(&[100.0, 100.0, 100.0, 100.0, 1.0], 5, 2.0, 0.0).expect("computes");
        assert_eq!(low_last.band_percent, 0.0);
    }

    #[test]
    fn indicator_helpers_should_compute_slope_and_regime_boundaries() {
        assert_eq!(compute_slope_metric(&[], 20), 0.0);
        assert_eq!(compute_slope_metric(&[1.0], 20), 0.0);
        assert_eq!(compute_slope_metric(&[0.0, 10.0], 20), 0.0);
        assert_eq!(compute_slope_metric(&[1.0, 3.0], 20), 1.0);
        assert_eq!(compute_slope_metric(&[100.0, -100.0], 20), -1.0);
        assert_eq!(compute_slope_metric(&[3.0, 4.0], 0), 0.0);

        assert_eq!(classify_regime(0.02, 0.5, 0.07), MarketRegime::Trending);
        assert_eq!(classify_regime(0.015, 0.02, 0.08), MarketRegime::Ranging);
        assert_eq!(classify_regime(0.0, 0.0201, 0.5), MarketRegime::Extreme);
        assert_eq!(classify_regime(0.0, -1.0, 0.5), MarketRegime::Ranging);
    }

    #[test]
    fn indicator_helpers_should_compute_fib_ofi_and_tilt_windows() {
        assert_eq!(compute_fib_anchors(&[]), TrendFibAnchors::default());

        let candles: Vec<_> = (0..130)
            .map(|idx| {
                let mut candle = sample_candle(idx, 100.0 + idx as f64);
                candle.high = idx as f64;
                candle.low = idx as f64;
                candle.open = idx as f64;
                candle.close = idx as f64;
                candle.volume = 1.0;
                candle
            })
            .collect();
        let fib = compute_fib_anchors(&candles);
        assert_eq!(fib.swing_low, 10.0);
        assert_eq!(fib.swing_high, 129.0);
        assert_eq!(fib.level_500, 69.5);

        let flat = compute_fib_anchors(&[sample_candle(0, 10.0)]);
        assert!(flat.level_382 < flat.swing_high);
        assert!(flat.level_618 < flat.level_382);

        let mut ofi_candles: Vec<_> = (0..6).map(|idx| sample_candle(idx, 10.0)).collect();
        for (idx, candle) in ofi_candles.iter_mut().enumerate() {
            candle.open = idx as f64;
            candle.close = idx as f64 + 1.0;
            candle.high = idx as f64 + 2.0;
            candle.low = idx as f64;
            candle.volume = 10.0;
        }
        assert_eq!(compute_ofi(&ofi_candles), 25.0);

        let mut tilt_candles: Vec<_> = (0..12).map(|idx| sample_candle(idx, 10.0)).collect();
        for (idx, candle) in tilt_candles.iter_mut().enumerate() {
            candle.open = 10.0;
            candle.close = if idx < 2 || idx >= 7 { 11.0 } else { 9.0 };
        }
        assert_eq!(compute_orderbook_tilt(&tilt_candles[..2]), 0.0);
        assert_eq!(compute_orderbook_tilt(&tilt_candles), 0.0);

        for candle in &mut tilt_candles {
            candle.close = 11.0;
        }
        assert_eq!(compute_orderbook_tilt(&tilt_candles), 1.0);
        for candle in &mut tilt_candles {
            candle.close = 9.0;
        }
        assert_eq!(compute_orderbook_tilt(&tilt_candles), -1.0);
    }

    #[test]
    fn indicator_history_and_data_quality_should_match_legacy_penalties() {
        let mut indicator = base_config().indicator_config;
        let market_data = MarketDataConfig {
            cache_depth: 10,
            bootstrap_bars: 10,
            max_data_lag_ms: 200,
            min_one_minute_bars: 3,
            min_five_minute_bars: 2,
            min_fifteen_minute_bars: 2,
            min_one_hour_bars: 2,
        };
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 1, 0, 0).unwrap();
        let mut snapshot = sample_candle_snapshot(now, 3, 2, 2, 0);

        assert!(!requires_one_hour_history(&indicator));
        assert!(has_minimum_history(&market_data, &indicator, &snapshot));

        indicator.primary_timeframe = "1H".to_string();
        assert!(requires_one_hour_history(&indicator));
        assert!(!has_minimum_history(&market_data, &indicator, &snapshot));

        indicator.primary_timeframe = " 1h".to_string();
        assert!(!requires_one_hour_history(&indicator));

        indicator.primary_timeframe = "1h".to_string();
        snapshot.one_minute.truncate(1);
        snapshot.one_minute[0].close_time = now;
        snapshot.five_minutes.truncate(1);
        snapshot.fifteen_minutes.truncate(1);
        let score = compute_data_quality(&market_data, &indicator, &snapshot, 201, now);
        assert_eq!(score, 20.0);

        snapshot.one_minute.clear();
        let no_last_score = compute_data_quality(&market_data, &indicator, &snapshot, 201, now);
        assert_eq!(no_last_score, 20.0);

        let boundary = sample_candle_snapshot(now - chrono::Duration::seconds(5), 3, 2, 2, 2);
        let score_at_boundary = compute_data_quality(&market_data, &indicator, &boundary, 0, now);
        assert_eq!(score_at_boundary, 100.0);

        let stale = sample_candle_snapshot(now - chrono::Duration::seconds(6), 3, 2, 2, 2);
        let stale_score = compute_data_quality(&market_data, &indicator, &stale, 0, now);
        assert_eq!(stale_score, 80.0);

        let impossible = MarketDataConfig {
            min_one_minute_bars: 100,
            min_five_minute_bars: 100,
            min_fifteen_minute_bars: 100,
            min_one_hour_bars: 100,
            ..market_data
        };
        let clamped = compute_data_quality(&impossible, &indicator, &stale, 999, now);
        assert_eq!(clamped, 0.0);
    }

    #[test]
    fn indicator_outputs_should_compose_bollinger_risk_and_volume_without_adapters() {
        let config = base_config().indicator_config;
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 2, 0, 0).unwrap();
        let snapshot = rich_candle_snapshot(now, 40, 30, 30, 0);

        let outputs = compute_indicator_outputs(&config, &snapshot, now).expect("outputs compute");

        assert_eq!(
            outputs.timestamp,
            snapshot.fifteen_minutes.last().unwrap().close_time
        );
        assert_eq!(
            outputs.last_price,
            snapshot.five_minutes.last().unwrap().close
        );
        assert!(outputs.bollinger_5m.upper > outputs.bollinger_5m.middle);
        assert!(outputs.bollinger_15m.middle > 0.0);
        assert!(outputs.rsi > 50.0);
        assert!(outputs.atr > 0.0);
        assert!(outputs.adx >= 0.0);
        assert_eq!(outputs.bbw_percentile, 0.5);
        assert_eq!(outputs.choppiness, None);
        assert_eq!(outputs.volume_window_minutes, 60);
        let expected_volume: f64 = snapshot
            .five_minutes
            .iter()
            .rev()
            .take(12)
            .map(|candle| candle.quote_volume)
            .sum();
        assert_eq!(outputs.recent_volume_quote, expected_volume);

        let sparse = sample_candle_snapshot(now, 1, 4, 4, 0);
        let fallback = compute_indicator_outputs(&config, &sparse, now).expect("fallback outputs");
        assert_eq!(
            fallback.bollinger_5m.middle,
            sparse.five_minutes.last().unwrap().close
        );
        assert_eq!(fallback.rsi, 50.0);
        assert_eq!(fallback.atr, 0.0);
        assert_eq!(fallback.adx, 20.0);
    }

    #[test]
    fn trend_snapshot_should_compose_adapter_free_trend_features() {
        let config = base_config();
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 3, 0, 0).unwrap();
        let snapshot = rich_candle_snapshot(now, 40, 30, 30, 2);
        let outputs =
            compute_indicator_outputs(&config.indicator_config, &snapshot, now).expect("outputs");
        let event = CandleEvent {
            symbol: "BTC/USDC".to_string(),
            interval: CandleInterval::OneMinute,
            candle: snapshot.one_minute.last().unwrap().clone(),
            is_final: true,
            latency_ms: 10,
        };

        let trend = compute_trend_snapshot(
            &config.indicator_config,
            &MarketDataConfig {
                cache_depth: 100,
                bootstrap_bars: 100,
                max_data_lag_ms: 200,
                min_one_minute_bars: 20,
                min_five_minute_bars: 20,
                min_fifteen_minute_bars: 20,
                min_one_hour_bars: 2,
            },
            &snapshot,
            &outputs,
            &event,
            now,
        );

        assert!(trend.ema_fast > 0.0);
        assert!(trend.ema_slow > 0.0);
        assert!(trend.atr_5m > 0.0);
        assert!(trend.atr_15m > 0.0);
        assert_eq!(trend.boll_mid, outputs.bollinger_5m.middle);
        assert_eq!(trend.boll_bandwidth, outputs.bbw);
        assert!(trend.fib_anchors.swing_high >= trend.fib_anchors.swing_low);
        assert!(trend.ofi > 0.0);
        assert!(trend.orderbook_imbalance > 0.0);
        assert_eq!(trend.data_quality_score, 100.0);
        assert_eq!(
            trend.regime,
            classify_regime(trend.ema_slope_5m, trend.atr_5m, outputs.bbw)
        );
        assert_eq!(trend.updated_at, event.candle.close_time);
    }

    #[test]
    fn indicator_event_core_should_filter_and_build_publish_decisions() {
        let config = base_config();
        let market_data = MarketDataConfig {
            cache_depth: 100,
            bootstrap_bars: 100,
            max_data_lag_ms: 200,
            min_one_minute_bars: 20,
            min_five_minute_bars: 20,
            min_fifteen_minute_bars: 20,
            min_one_hour_bars: 0,
        };
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 4, 0, 0).unwrap();
        let snapshot = rich_candle_snapshot(now, 40, 30, 30, 0);
        let symbols = vec!["BTC/USDC".to_string()];
        let event = CandleEvent {
            symbol: "BTC/USDC".to_string(),
            interval: CandleInterval::OneMinute,
            candle: snapshot.one_minute.last().unwrap().clone(),
            is_final: true,
            latency_ms: 10,
        };

        let unknown = CandleEvent {
            symbol: "ETH/USDC".to_string(),
            ..event.clone()
        };
        assert_eq!(
            handle_indicator_event_core(
                &symbols,
                &config.indicator_config,
                &market_data,
                &unknown,
                Some(&snapshot),
                now,
            )
            .expect_err("unknown symbol skipped"),
            IndicatorEventSkipReason::UnknownSymbol
        );

        let non_final_high_tf = CandleEvent {
            interval: CandleInterval::FiveMinutes,
            is_final: false,
            ..event.clone()
        };
        assert_eq!(
            handle_indicator_event_core(
                &symbols,
                &config.indicator_config,
                &market_data,
                &non_final_high_tf,
                Some(&snapshot),
                now,
            )
            .expect_err("non-final higher timeframe skipped"),
            IndicatorEventSkipReason::NonFinalHigherTimeframe
        );

        assert_eq!(
            handle_indicator_event_core(
                &symbols,
                &config.indicator_config,
                &market_data,
                &event,
                None,
                now,
            )
            .expect_err("missing snapshot skipped"),
            IndicatorEventSkipReason::MissingSnapshot
        );

        let sparse = sample_candle_snapshot(now, 1, 1, 1, 0);
        assert_eq!(
            handle_indicator_event_core(
                &symbols,
                &config.indicator_config,
                &market_data,
                &event,
                Some(&sparse),
                now,
            )
            .expect_err("insufficient history skipped"),
            IndicatorEventSkipReason::InsufficientHistory
        );

        let decision = handle_indicator_event_core(
            &symbols,
            &config.indicator_config,
            &market_data,
            &event,
            Some(&snapshot),
            now,
        )
        .expect("event produces publish decision");
        assert_eq!(decision.symbol, "BTC/USDC");
        assert_eq!(decision.publish_at, event.candle.close_time);
        assert_eq!(decision.trend_snapshot.updated_at, event.candle.close_time);
        assert_eq!(
            decision.trend_snapshot.boll_mid,
            decision.indicators.bollinger_5m.middle
        );
    }

    #[test]
    fn monitoring_core_should_update_metrics_drawdown_and_reports() {
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 5, 0, 0).unwrap();
        let mut metrics = PerformanceMetrics::default();
        let mut high_water_mark = 0.0;
        let trades = vec![
            monitor_trade("BTC/USDC", 10.0, 60, now),
            monitor_trade("ETH/USDC", -4.0, 120, now + chrono::Duration::minutes(1)),
            monitor_trade("SOL/USDC", 6.0, 180, now + chrono::Duration::minutes(2)),
        ];

        for trade in &trades {
            record_monitor_trade(&mut metrics, &mut high_water_mark, trade, trade.exit_time);
        }
        refresh_monitor_statistics(&mut metrics, &trades);

        assert_eq!(metrics.total_trades, 3);
        assert_eq!(metrics.winning_trades, 2);
        assert_eq!(metrics.losing_trades, 1);
        assert_eq!(metrics.total_pnl, 12.0);
        assert!((metrics.win_rate - 2.0 / 3.0).abs() < 1e-12);
        assert_eq!(metrics.avg_win, 8.0);
        assert_eq!(metrics.avg_loss, 4.0);
        assert_eq!(metrics.largest_win, 10.0);
        assert_eq!(metrics.largest_loss, -4.0);
        assert_eq!(metrics.profit_factor, 2.0);
        assert_eq!(metrics.avg_holding_time, 120.0);
        assert_eq!(high_water_mark, 12.0);
        assert!((metrics.max_drawdown - 0.4).abs() < 1e-12);
        assert!((metrics.current_drawdown - 0.0).abs() < 1e-12);
        assert_eq!(metrics.recovery_factor, 30.0);
        assert!(metrics.sharpe_ratio > 0.0);
        assert_eq!(metrics.last_update, trades.last().unwrap().exit_time);

        let report =
            build_performance_report(&metrics, &trades, 2, now + chrono::Duration::minutes(3));
        assert_eq!(report.metrics.total_trades, 3);
        assert_eq!(report.recent_trades.len(), 2);
        assert_eq!(report.recent_trades[0].symbol, "SOL/USDC");
        assert_eq!(report.recent_trades[1].symbol, "ETH/USDC");
        assert_eq!(calculate_monitor_sharpe_ratio(&[1.0]), 0.0);
        assert_eq!(calculate_monitor_sharpe_ratio(&[2.0, 2.0]), 0.0);
    }

    #[test]
    fn execution_core_should_quantize_prices_amounts_and_slippage() {
        let precision = SymbolPrecision {
            qty_precision: 3,
            price_precision: 2,
            step_size: 0.001,
            tick_size: 0.05,
            min_notional: None,
        };

        assert_eq!(quantize_execution_amount(1.23456, &precision), 1.234);
        assert_eq!(quantize_execution_price(100.026, &precision), 100.05);
        assert_eq!(
            quantize_execution_maker_price(100.026, &precision, &OrderSide::Buy),
            100.0
        );
        assert_eq!(
            quantize_execution_maker_price(100.026, &precision, &OrderSide::Sell),
            100.05
        );

        let fallback = SymbolPrecision {
            qty_precision: 2,
            price_precision: 2,
            step_size: 0.0,
            tick_size: 0.0,
            min_notional: None,
        };
        assert_eq!(quantize_execution_amount(1.239, &fallback), 1.23);
        assert_eq!(quantize_execution_price(1.235, &fallback), 1.24);
        assert_eq!(
            quantize_execution_maker_price(1.239, &fallback, &OrderSide::Buy),
            1.23
        );
        assert_eq!(
            quantize_execution_maker_price(1.231, &fallback, &OrderSide::Sell),
            1.24
        );
        assert_eq!(compute_execution_slippage(0.0, 101.0), 0.0);
        assert!((compute_execution_slippage(100.0, 101.0) - 0.01).abs() < 1e-12);
        assert_eq!(execution_decimals_from_step(0.001), 3);
        assert_eq!(execution_decimals_from_step(0.0), 3);
        assert_eq!(format_execution_decimal(1.2, 3), "1.200");
        assert_eq!(format_execution_decimal(1.2, 0), "1");
        assert_eq!(
            build_trend_entry_client_id("BTC/USDC", 123),
            "trdent_btcusdc_123"
        );
        assert!(build_trend_entry_client_id("VERY-LONG-SYMBOL/USDC-PERP", 123456789).len() <= 30);
    }

    #[test]
    fn execution_core_should_build_maker_and_market_order_plans() {
        let mut execution = base_config().execution;
        execution.maker_offset_bps = 10.0;
        let precision = SymbolPrecision {
            qty_precision: 3,
            price_precision: 2,
            step_size: 0.001,
            tick_size: 0.05,
            min_notional: None,
        };
        let buy = sample_signal("BTC/USDC", OrderSide::Buy, 100.0, 95.0);
        let sell = sample_signal("BTC/USDC", OrderSide::Sell, 100.0, 105.0);

        let maker = build_trend_execution_order_plan(
            &execution, &buy, 1.23456, &precision, true, "client-1", true,
        );
        assert_eq!(maker.symbol, "BTC/USDC");
        assert_eq!(maker.side, OrderSide::Buy);
        assert_eq!(maker.order_type, OrderType::Limit);
        assert_eq!(maker.quantity, 1.234);
        assert_eq!(maker.price, Some(99.9));
        assert!(maker.post_only);
        assert_eq!(maker.time_in_force, Some(TimeInForce::GoodTilCanceled));
        assert_eq!(maker.client_order_id, "client-1");
        assert_eq!(maker.position_side.as_deref(), Some("LONG"));

        let sell_maker = build_trend_execution_order_plan(
            &execution, &sell, 1.0, &precision, true, "client-2", true,
        );
        assert_eq!(sell_maker.price, Some(100.1));
        assert_eq!(sell_maker.position_side.as_deref(), Some("SHORT"));

        let market = build_trend_execution_order_plan(
            &execution,
            &buy,
            0.0,
            &SymbolPrecision::default(),
            false,
            "client-3",
            false,
        );
        assert_eq!(market.order_type, OrderType::Market);
        assert_eq!(market.quantity, 0.1);
        assert_eq!(market.price, None);
        assert!(!market.post_only);
        assert_eq!(market.time_in_force, None);
        assert_eq!(market.position_side, None);
    }

    fn base_config() -> TrendConfig {
        serde_yaml::from_str(CONFIG_YAML).expect("config parses")
    }

    fn sample_signal(symbol: &str, side: OrderSide, entry: f64, stop: f64) -> TradeSignal {
        TradeSignal {
            symbol: symbol.to_string(),
            signal_type: SignalType::TrendBreakout,
            side: side.clone(),
            entry_price: entry,
            stop_loss: stop,
            take_profits: calculate_take_profits(entry, stop, side),
            suggested_size: 10.0,
            risk_reward_ratio: 4.0,
            confidence: 80.0,
            timeframe_aligned: true,
            has_structure_support: true,
            expire_time: Utc::now(),
            metadata: SignalMetadata {
                trend_strength: 70.0,
                volume_confirmed: true,
                key_level_nearby: true,
                pattern_name: None,
                generated_at: Utc::now(),
            },
        }
    }

    fn sample_position(
        symbol: &str,
        side: OrderSide,
        entry: f64,
        stop: f64,
        size: f64,
    ) -> TrendPosition {
        let now = Utc::now();
        TrendPosition {
            symbol: symbol.to_string(),
            side,
            entry_price: entry,
            average_price: entry,
            current_price: entry,
            size,
            current_size: size,
            initial_size: size,
            pyramid_entries: Vec::new(),
            pyramid_count: 0,
            stop_loss: stop,
            take_profits: vec![TakeProfitLevel {
                price: entry + (entry - stop).abs(),
                ratio: 0.3,
                executed: false,
            }],
            entry_time: now,
            last_update: now,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            peak_profit: 0.0,
            risk_amount: size * (entry - stop).abs(),
        }
    }

    fn sample_candle(offset_minutes: i64, close: f64) -> TrendCandle {
        let open_time = Utc.with_ymd_and_hms(2026, 6, 7, 0, 0, 0).unwrap()
            + chrono::Duration::minutes(offset_minutes);
        TrendCandle {
            open_time,
            close_time: open_time + chrono::Duration::minutes(1),
            open: close - 0.5,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 10.0,
            quote_volume: close * 10.0,
            trade_count: 12,
        }
    }

    fn sample_candle_event(symbol: &str, candle: TrendCandle) -> CandleEvent {
        CandleEvent {
            symbol: symbol.to_string(),
            interval: CandleInterval::OneMinute,
            candle,
            is_final: true,
            latency_ms: 10,
        }
    }

    fn sample_candle_snapshot(
        now: DateTime<Utc>,
        one_minute: usize,
        five_minutes: usize,
        fifteen_minutes: usize,
        one_hour: usize,
    ) -> CandleSnapshot {
        let make_candles = |count: usize, interval_minutes: i64| {
            (0..count)
                .map(|idx| {
                    let close_time = now
                        - chrono::Duration::minutes((count - idx - 1) as i64 * interval_minutes);
                    TrendCandle {
                        open_time: close_time - chrono::Duration::minutes(interval_minutes),
                        close_time,
                        open: 100.0,
                        high: 101.0,
                        low: 99.0,
                        close: 100.5,
                        volume: 10.0,
                        quote_volume: 1005.0,
                        trade_count: 10,
                    }
                })
                .collect()
        };

        CandleSnapshot {
            one_minute: make_candles(one_minute, 1),
            five_minutes: make_candles(five_minutes, 5),
            fifteen_minutes: make_candles(fifteen_minutes, 15),
            one_hour: make_candles(one_hour, 60),
        }
    }

    fn rich_candle_snapshot(
        now: DateTime<Utc>,
        one_minute: usize,
        five_minutes: usize,
        fifteen_minutes: usize,
        one_hour: usize,
    ) -> CandleSnapshot {
        let make_candles = |count: usize, interval_minutes: i64, start: f64| {
            (0..count)
                .map(|idx| {
                    let close_time = now
                        - chrono::Duration::minutes((count - idx - 1) as i64 * interval_minutes);
                    let base = start + idx as f64 * 0.2;
                    TrendCandle {
                        open_time: close_time - chrono::Duration::minutes(interval_minutes),
                        close_time,
                        open: base,
                        high: base + 1.5,
                        low: base - 0.5,
                        close: base + 1.0,
                        volume: 10.0 + idx as f64,
                        quote_volume: (base + 1.0) * (10.0 + idx as f64),
                        trade_count: 10 + idx as u64,
                    }
                })
                .collect()
        };

        CandleSnapshot {
            one_minute: make_candles(one_minute, 1, 100.0),
            five_minutes: make_candles(five_minutes, 5, 100.0),
            fifteen_minutes: make_candles(fifteen_minutes, 15, 100.0),
            one_hour: make_candles(one_hour, 60, 100.0),
        }
    }

    fn monitor_trade(
        symbol: &str,
        pnl: f64,
        holding_time: i64,
        exit_time: DateTime<Utc>,
    ) -> MonitorTradeRecord {
        MonitorTradeRecord {
            symbol: symbol.to_string(),
            side: if pnl >= 0.0 { "long" } else { "short" }.to_string(),
            entry_price: 100.0,
            exit_price: 100.0 + pnl,
            size: 1.0,
            pnl,
            entry_time: exit_time - chrono::Duration::seconds(holding_time),
            exit_time,
            holding_time,
            exit_reason: "test".to_string(),
        }
    }

    fn shared_data() -> SharedTrendData {
        SharedTrendData {
            indicators: IndicatorSnapshot {
                last_price: 100.0,
                atr: 2.0,
                bollinger_middle: 100.0,
            },
            trend_snapshot: Some(TrendIndicatorSnapshot {
                ema_fast: 101.0,
                ema_slow: 99.0,
                fib_anchors: TrendFibAnchors {
                    swing_high: 110.0,
                    swing_low: 90.0,
                    level_382: 102.36,
                    level_500: 100.0,
                    level_618: 97.64,
                },
                regime: MarketRegime::Trending,
                ..Default::default()
            }),
        }
    }

    fn risk_snapshot() -> RiskSnapshot {
        RiskSnapshot {
            system: SystemHealth {
                api_latency: 100.0,
                network_status: true,
                exchange_status: true,
                data_feed_status: true,
                error_count: 0,
            },
            account: AccountMetrics {
                total_equity: 1000.0,
                available_balance: 900.0,
                used_margin: 100.0,
                daily_pnl: 0.0,
                current_drawdown: 0.0,
                total_exposure: 0.1,
                leverage_used: 0.1,
            },
            strategy: StrategyMetrics {
                open_positions: 1,
                daily_trades: 1,
                consecutive_losses: 0,
                win_rate: 0.5,
                largest_loss: 0.0,
            },
            order: OrderMetrics {
                order_success_rate: 1.0,
                max_slippage: 0.0,
                avg_execution_time: 100.0,
                failed_orders: 0,
            },
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
name: Trend Following Strategy
account_id: account-1
dual_position_mode: true
symbols:
  - BTC/USDC
  - ETH/USDC
max_positions: 3
max_daily_trades: 10
min_account_balance: 1000.0
min_risk_reward_ratio: 2.0
min_signal_confidence: 60.0
max_holding_hours: 24
pyramid_enabled: true
pyramid_levels:
  - trigger_profit: 0.5
    size_ratio: 0.3
risk_config:
  max_risk_per_trade: 0.01
  max_daily_loss: 0.03
  max_drawdown: 0.15
  max_consecutive_losses: 5
  max_total_exposure: 0.5
  max_single_exposure: 0.1
  max_correlated_exposure: 0.2
  max_leverage: 3.0
  max_slippage: 0.002
  emergency_stop_loss: 0.1
indicator_config:
  primary_timeframe: 15m
  secondary_timeframe: 5m
  trigger_timeframe: 1m
  fast_ema: 20
  slow_ema: 50
  atr_period: 14
  adx_period: 14
  adx_threshold: 25.0
  rsi_period: 14
  rsi_overbought: 70.0
  rsi_oversold: 30.0
  macd_fast: 12
  macd_slow: 26
  macd_signal: 9
  bb_period: 20
  bb_std: 2.0
signal_config:
  breakout_lookback: 20
  breakout_confirmation_bars: 2
  pullback_ratio: 0.382
  pullback_min_trend_strength: 40.0
  momentum_threshold: 0.02
  momentum_lookback: 10
  pattern_min_bars: 5
  pattern_confidence: 70.0
  volume_multiplier: 1.5
  volume_lookback: 20
  signal_expiry: 300
position_config:
  base_risk_ratio: 0.01
  kelly_fraction: 0.25
  pyramid_enabled: true
  pyramid_levels:
    - trigger_profit: 0.5
      size_ratio: 0.3
  trend_factor_enabled: true
  volatility_factor_enabled: true
  time_factor_enabled: true
  max_holding_hours: 24
stop_config:
  initial_stop_type: ATR
  atr_multiplier: 2.0
  fixed_stop_percent: 0.02
  trailing_stop_enabled: true
  trailing_activation: 1.0
  trailing_distance: 0.5
  trailing_step: 0.5
  time_stop_hours: 24
  breakeven_enabled: true
  breakeven_trigger: 0.5
  partial_targets:
    - target_r: 1.0
      close_ratio: 0.3
entry_leverage: 1.0
entry_allocation:
  ma: 0.6
  fib: 0.25
  bb: 0.15
allocation_regimes:
  trending:
    ma: 0.6
    fib: 0.25
    bb: 0.15
  extreme:
    ma: 0.7
    fib: 0.2
    bb: 0.1
  ranging:
    ma: 0.2
    fib: 0.3
    bb: 0.5
execution:
  max_retries: 3
market_data:
  cache_depth: 500
  bootstrap_bars: 200
  min_one_minute_bars: 60
  min_one_hour_bars: 24
"#;
}
