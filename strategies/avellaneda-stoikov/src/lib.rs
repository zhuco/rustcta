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
    apply_fill_to_state, build_risk_snapshot, calculate_optimal_spread, calculate_order_qty,
    calculate_reservation_price, calculate_volatility, generate_quotes,
    liquidity_spread_multiplier, refresh_unrealized_pnl, symbol_matches, ASCoreConfig,
    ASOrderState, ASOrderStatus, ASParameters, ASQuote, ASRiskSnapshot, ASState, AccountConfig,
    AlertThresholds, AlertsConfig, DataSourcesConfig, DebugConfig, EmergencyStopConfig,
    FundingRateConfig, HealthCheckConfig, InventoryRiskConfig, InventorySkewConfig, KlineConfig,
    LiquidityCheckConfig, MaintenanceConfig, MarketDataConfig, MarketRules, MarketSpecificConfig,
    MetricsConfig, MonitoringConfig, OrderAggregationConfig, OrderConfig, PartialFillConfig,
    PerformanceConfig, PerpetualConfig, RestApiConfig, RiskConfig, SmartCancellationConfig,
    SpreadAdjustmentConfig, StopLossConfig, StrategyConfig, TradeLoggingConfig, TradingConfig,
    VolatilityConfig, VolatilityHandlingConfig, VolatilityLimitsConfig, VolatilityMultiplierConfig,
    WebSocketConfig,
};

pub const STRATEGY_KIND: &str = "avellaneda_stoikov";
pub const DISPLAY_NAME: &str = "Avellaneda Stoikov";
pub const MIGRATED_FROM: &str = "src/strategies/avellaneda_stoikov";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvellanedaStoikovStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for AvellanedaStoikovStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AvellanedaStoikovConfig {
    pub symbol: String,
    pub order_size_usdc: String,
    pub max_inventory: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvellanedaStoikovSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct AvellanedaStoikovRuntime {
    instance_id: StrategyInstanceId,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
}

impl AvellanedaStoikovRuntime {
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

    fn snapshot_payload(&self) -> AvellanedaStoikovSnapshotPayload {
        AvellanedaStoikovSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
        }
    }
}

impl Default for AvellanedaStoikovRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for AvellanedaStoikovRuntime {
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
            "Partially migrated Avellaneda-Stoikov strategy with adapter-free quote core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: Vec::new(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places market-making limit orders",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels stale market-making orders",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Controls inventory around the configured target ratio",
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
                    "market_rules",
                    "state_dto",
                    "reservation_price",
                    "optimal_spread",
                    "quote_generation",
                    "volatility",
                    "order_sizing",
                    "fill_accounting",
                    "risk_snapshot",
                    "liquidity_multiplier",
                    "symbol_matching"
                ]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!([
                    "controller",
                    "exchange_io",
                    "websocket",
                    "risk_evaluator_wiring",
                    "order_lifecycle",
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
            "required": ["symbol", "order_size_usdc", "max_inventory"],
            "properties": {
                "symbol": { "type": "string", "minLength": 1 },
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
        let mut runtime = AvellanedaStoikovRuntime::new();
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
                "symbol": "DCR/USDT"
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
    fn migrated_config_should_accept_market_specific_and_debug_sections() {
        let config: ASCoreConfig = serde_yaml::from_str(CONFIG_YAML).expect("config should parse");

        config.validate_core().expect("config should validate");
        assert!(config.market_specific.is_some());
        assert!(config.debug.expect("debug").dry_run);
    }

    #[test]
    fn migrated_market_rules_should_round_prices_and_quantities() {
        let rules = MarketRules::from_exchange(0.01, 0.001, 5.0, 0.001, 1000.0, MarketType::Spot);

        assert_eq!(rules.round_bid(19.997), 19.99);
        assert_eq!(rules.round_ask(19.991), 20.0);
        assert_eq!(rules.round_qty(0.1239), 0.123);
        assert_eq!(rules.price_digits, 2);
        assert_eq!(rules.qty_digits, 3);
    }

    #[test]
    fn migrated_quote_core_should_calculate_reservation_spread_and_quotes() {
        let mut config = base_config();
        config.trading.min_spread_bp = 10.0;
        config.trading.max_spread_bp = 100.0;
        let state = ASState {
            mid_price: 100.0,
            bid_price: 99.9,
            ask_price: 100.1,
            volatility: 0.2,
            inventory: 2.0,
            ..ASState::new(100.0)
        };
        let rules = MarketRules::from_exchange(0.01, 0.001, 5.0, 0.001, 1000.0, MarketType::Spot);

        let reservation = calculate_reservation_price(&config, &state);
        let spread = calculate_optimal_spread(&config, &state);
        let quote = generate_quotes(&config, &state, &rules, 1.0).expect("quote should build");

        assert!(reservation < state.mid_price);
        assert!((0.001..=0.01).contains(&spread));
        assert!(quote.bid < quote.ask);
        assert!(quote.bid <= state.mid_price);
        assert!(quote.ask >= state.mid_price);
    }

    #[test]
    fn migrated_quote_core_should_reject_invalid_mid_price() {
        let config = base_config();
        let state = ASState {
            mid_price: 0.0,
            ..ASState::new(0.0)
        };
        let rules = MarketRules::from_exchange(0.01, 0.001, 5.0, 0.001, 1000.0, MarketType::Spot);

        assert!(generate_quotes(&config, &state, &rules, 1.0).is_err());
    }

    #[test]
    fn migrated_quote_core_should_apply_liquidity_multiplier() {
        let config = base_config();
        let liquidity = config
            .market_specific
            .as_ref()
            .and_then(|market_specific| market_specific.liquidity_check.as_ref());

        assert_eq!(liquidity_spread_multiplier(liquidity, 0.0), 1.0);
        assert_eq!(liquidity_spread_multiplier(liquidity, 10_000.0), 1.0);
        assert_eq!(liquidity_spread_multiplier(liquidity, 1_000.0), 1.5);
    }

    #[test]
    fn migrated_volatility_should_fallback_and_annualize_weighted_returns() {
        let config = base_config();
        assert_eq!(
            calculate_volatility(
                &[100.0],
                &config.as_params.volatility,
                &config.risk.volatility_limits
            ),
            0.2
        );
        let vol = calculate_volatility(
            &[100.0, 101.0, 100.5, 102.0],
            &config.as_params.volatility,
            &config.risk.volatility_limits,
        );
        assert!(vol > 0.0001);
    }

    #[test]
    fn migrated_order_sizing_should_respect_min_notional_and_steps() {
        let rules = MarketRules::from_exchange(0.01, 0.001, 5.0, 0.001, 1000.0, MarketType::Spot);
        let price = rules.round_bid(19.997);
        let qty = calculate_order_qty(5.0, price, &rules).expect("qty should build");

        assert_eq!(price, 19.99);
        assert!(qty * price >= 5.0);
        assert!((qty * 1000.0).fract().abs() < 1e-9);
    }

    #[test]
    fn migrated_fill_core_should_track_inventory_average_cost_and_realized_pnl() {
        let mut state = ASState {
            mid_price: 100.0,
            volatility: 0.2,
            ..ASState::new(100.0)
        };

        apply_fill_to_state(&mut state, OrderSide::Buy, 2.0, 100.0, 0.01);
        assert_eq!(state.inventory, 2.0);
        assert_eq!(state.avg_entry_price, 100.0);
        assert_eq!(state.buy_fills, 1);
        assert!((state.realized_pnl + 0.01).abs() < 1e-9);

        apply_fill_to_state(&mut state, OrderSide::Sell, 1.5, 110.0, 0.02);
        assert!((state.inventory - 0.5).abs() < 1e-9);
        assert!((state.realized_pnl - 14.97).abs() < 1e-9);
        assert_eq!(state.sell_fills, 1);
        assert_eq!(state.total_trades, 2);
    }

    #[test]
    fn migrated_fill_core_should_set_avg_entry_to_reversal_price() {
        let mut state = ASState {
            mid_price: 100.0,
            ..ASState::new(100.0)
        };

        apply_fill_to_state(&mut state, OrderSide::Buy, 1.0, 100.0, 0.0);
        apply_fill_to_state(&mut state, OrderSide::Sell, 2.0, 90.0, 0.0);

        assert_eq!(state.inventory, -1.0);
        assert_eq!(state.avg_entry_price, 90.0);
        assert!((state.realized_pnl + 10.0).abs() < 1e-9);
    }

    #[test]
    fn migrated_risk_snapshot_should_use_absolute_inventory_value() {
        let config = base_config();
        let mut state = ASState::new(100.0);
        state.inventory_value = -900.0;

        let snapshot = build_risk_snapshot(&config, &state);

        assert_eq!(snapshot.inventory_ratio, Some(0.9));
        assert_eq!(snapshot.notional, -900.0);
    }

    #[test]
    fn migrated_state_helpers_should_mark_terminal_orders_and_match_symbols() {
        let now = Utc::now();
        let open = ASOrderState {
            exchange_order_id: "1".to_string(),
            client_order_id: Some("as-1".to_string()),
            side: OrderSide::Buy,
            price: 100.0,
            original_qty: 1.0,
            filled_qty: 0.0,
            status: ASOrderStatus::Open,
            created_at: now,
            updated_at: now,
        };
        let mut closed = open.clone();
        closed.status = ASOrderStatus::Closed;

        assert!(!open.is_terminal());
        assert!(closed.is_terminal());
        assert!(symbol_matches("DCRUSDT", "DCR/USDT"));
        assert!(symbol_matches("dcr/usdt", "DCR/USDT"));
        assert!(!symbol_matches("BTCUSDT", "DCR/USDT"));
    }

    #[test]
    fn migrated_config_validation_should_reject_invalid_as_parameters() {
        let mut config = base_config();
        config.as_params.risk_aversion = 0.0;
        assert!(config.validate_core().is_err());

        let mut config = base_config();
        config.as_params.volatility.lookback_periods = 1;
        assert!(config.validate_core().is_err());

        let mut config = base_config();
        config.as_params.volatility.decay_factor = 1.1;
        assert!(config.validate_core().is_err());
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

    fn base_config() -> ASCoreConfig {
        serde_yaml::from_str(CONFIG_YAML).expect("config should parse")
    }

    const CONFIG_YAML: &str = r#"
strategy:
  name: "AS_CFG"
  version: "1.0"
  type: "avellaneda_stoikov"
  description: "cfg"
  enabled: true
  log_level: "INFO"
account:
  account_id: "test"
  exchange: "paper"
trading:
  symbol: "DCR/USDT"
  market_type: "spot"
  order_size_usdc: 5.0
  max_inventory: 10.0
  min_spread_bp: 10.0
  max_spread_bp: 100.0
  refresh_interval_secs: 1
  price_precision: 2
  quantity_precision: 3
  order_config:
    post_only: true
    time_in_force: "GTC"
    reduce_only: false
as_params:
  risk_aversion: 0.8
  order_book_intensity: 1.5
  time_horizon_seconds: 900
  volatility:
    lookback_periods: 3
    update_interval: 20
    decay_factor: 0.9
  inventory_skew:
    enabled: true
    skew_factor: 0.5
    target_inventory_ratio: 0.0
  spread_adjustment:
    volume_factor: 0.0
    depth_factor: 0.0
    pressure_factor: 0.0
market_data:
  orderbook_levels: 5
  trades_buffer_size: 100
  kline:
    interval: "1m"
    history_size: 10
risk:
  max_unrealized_loss: 100.0
  max_daily_loss: 100.0
  inventory_risk:
    max_position_value: 1000.0
    imbalance_threshold: 0.8
  stop_loss:
    enabled: true
    stop_loss_pct: 0.05
    cooldown_seconds: 60
  volatility_limits:
    max_volatility: 300.0
    min_volatility: 20.0
  emergency_stop:
    consecutive_losses: 3
    max_drawdown_pct: 0.1
performance:
  order_aggregation:
    enabled: false
    min_price_diff_bp: 1.0
  smart_cancellation:
    enabled: true
    price_drift_threshold_bp: 10.0
  partial_fill:
    min_fill_ratio: 0.1
    immediate_replace: true
data_sources:
  websocket:
    enabled: false
    streams: []
    reconnect_interval: 5
  rest_api:
    enabled: true
    refresh_interval: 30
    endpoints: []
monitoring:
  metrics:
    enabled: false
    export_interval: 60
  health_check:
    enabled: false
    interval: 30
  trade_logging:
    enabled: false
    log_fills: false
    log_quotes: false
    log_cancellations: false
  alerts:
    enabled: false
    channels: []
    thresholds:
      inventory_imbalance: 0.5
      loss_limit: 0.5
      low_liquidity: 1
perpetual:
  funding_rate:
    consider_funding: false
    threshold_pct: 0.0
  maintenance:
    stop_before: 0
    resume_after: 0
market_specific:
  volatility_handling:
    adaptive_spread: true
    volatility_multiplier:
      low: 1.0
      medium: 1.3
      high: 1.8
  liquidity_check:
    enabled: true
    min_depth_usd: 5000.0
    low_liquidity_spread_multiplier: 1.5
debug:
  enabled: true
  verbose_logging: false
  dry_run: true
  save_market_data: false
"#;
}
