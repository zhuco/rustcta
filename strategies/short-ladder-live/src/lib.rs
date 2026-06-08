use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, ExecutionCancelCommand, ExecutionOrderCommand, HealthSeverity,
    MarketDataChannel, MarketDataSubscription, MarketType, OrderSide, OrderType,
    RequiredAccountPermission, RiskCapability, RiskCapabilityDeclaration, StrategyCommandSchema,
    StrategyConfigSchema, StrategyContext, StrategyEvent, StrategyHealthIssue, StrategyInstanceId,
    StrategyRuntime, StrategySnapshot, StrategySnapshotSchema, StrategySpec, StrategyStatus,
    TimeInForce,
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
pub const MIGRATED_FROM: &str = "legacy-strategy:short_ladder_live";

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
    #[serde(default = "default_exchange_id")]
    pub exchange_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    #[serde(default = "default_short_ladder_candle_intervals")]
    pub candle_intervals: Vec<String>,
    pub initial_notional: String,
    pub max_notional: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShortLadderLiveSnapshotPayload {
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
    pub last_timer_at: Option<DateTime<Utc>>,
    pub last_event_digest: Option<RuntimeEventDigest>,
    pub task_signals: Vec<RuntimeTaskSignal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventDigest {
    pub event_kind: String,
    pub symbol: Option<String>,
    pub account_id: Option<String>,
    pub client_order_id: Option<String>,
    pub command_kind: Option<String>,
    pub timer_id: Option<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTaskSignal {
    pub task_kind: String,
    pub requested_at: DateTime<Utc>,
    #[serde(default)]
    pub payload: Value,
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
    market_data_events: u64,
    execution_events: u64,
    account_events: u64,
    operator_commands: u64,
    timer_events: u64,
    last_market_data_at: Option<DateTime<Utc>>,
    last_execution_at: Option<DateTime<Utc>>,
    last_account_sync_at: Option<DateTime<Utc>>,
    config: Option<ShortLadderLiveConfig>,
    market_data_subscriptions: Vec<MarketDataSubscription>,
    last_timer_at: Option<DateTime<Utc>>,
    last_event_digest: Option<RuntimeEventDigest>,
    task_signals: Vec<RuntimeTaskSignal>,
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
            last_timer_at: None,
            last_event_digest: None,
            task_signals: Vec::new(),
        }
    }

    fn snapshot_payload(&self) -> ShortLadderLiveSnapshotPayload {
        ShortLadderLiveSnapshotPayload {
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
            last_timer_at: self.last_timer_at,
            last_event_digest: self.last_event_digest.clone(),
            task_signals: self.task_signals.clone(),
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
        let config: ShortLadderLiveConfig = serde_json::from_value(ctx.config().clone())?;
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.market_data_subscriptions = short_ladder_market_data_subscriptions(&config);
        self.config = Some(config);
        self.last_event_at = Some(ctx.started_at());
        self.handled_events = 0;
        self.market_data_events = 0;
        self.execution_events = 0;
        self.account_events = 0;
        self.operator_commands = 0;
        self.timer_events = 0;
        self.last_timer_at = None;
        self.last_event_digest = None;
        self.task_signals.clear();
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.status = StrategyStatus::Stopped;
        Ok(())
    }

    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()> {
        self.handled_events += 1;
        self.last_event_at = Some(event_timestamp(&event));
        self.last_event_digest = Some(runtime_event_digest(&event));
        for signal in runtime_task_signals(&event) {
            self.task_signals.push(signal);
        }
        if self.task_signals.len() > 16 {
            let excess = self.task_signals.len() - 16;
            self.task_signals.drain(0..excess);
        }
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
            StrategyEvent::Timer(event) => {
                self.timer_events += 1;
                self.last_timer_at = Some(event.fired_at);
            }
        }
        Ok(())
    }

    async fn snapshot(&self) -> anyhow::Result<StrategySnapshot> {
        let captured_at = Utc::now();
        Ok(StrategySnapshot {
            schema_version: 1,
            instance_id: self.instance_id.clone(),
            strategy_kind: STRATEGY_KIND.to_string(),
            strategy_id: self.strategy_id.clone(),
            run_id: self.run_id.clone(),
            captured_at,
            status: self.status.clone(),
            payload: serde_json::to_value(self.snapshot_payload())?,
            health: runtime_health_issues(
                captured_at,
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
            "Partially migrated short ladder live strategy with adapter-free ladder model core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
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
                "migrated_runtime_contracts".to_string(),
                json!([
                    "sdk_context_config_loading",
                    "market_data_subscription_contract",
                    "execution_event_sync",
                    "account_event_sync",
                    "operator_command_orchestration",
                    "sanitized_runtime_snapshot"
                ]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!([
                    "market_signal",
                    "execution",
                    "tasks",
                    "logging",
                    "exchange_backed_runtime"
                ]),
            ),
            (
                "market_data_channels".to_string(),
                json!([
                    MarketDataChannel::Candles {
                        interval: "5m".to_string()
                    },
                    MarketDataChannel::OrderBookTop
                ]),
            ),
            (
                "primary_market_type".to_string(),
                json!(MarketType::Perpetual),
            ),
        ]),
    }
}

pub fn short_ladder_market_data_subscriptions(
    config: &ShortLadderLiveConfig,
) -> Vec<MarketDataSubscription> {
    let channels = config
        .candle_intervals
        .iter()
        .filter(|interval| !interval.trim().is_empty())
        .map(|interval| MarketDataChannel::Candles {
            interval: interval.trim().to_string(),
        })
        .chain(std::iter::once(MarketDataChannel::OrderBookTop))
        .collect::<Vec<_>>();

    config
        .symbols
        .iter()
        .filter(|symbol| !symbol.trim().is_empty())
        .map(|symbol| MarketDataSubscription {
            exchange_id: config.exchange_id.clone(),
            symbol: symbol.trim().to_string(),
            market_type: config.market_type.clone(),
            channels: channels.clone(),
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn short_ladder_entry_to_execution_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    account: &AccountConfig,
    symbol_cfg: &SymbolConfig,
    execution: &ExecutionConfig,
    precision: &SymbolPrecision,
    risk_profile_id: &str,
    layer_index: usize,
    notional: f64,
    reference_price: f64,
    requested_at: DateTime<Utc>,
) -> Option<ExecutionOrderCommand> {
    let quantity = derive_order_quantity_from_precision(precision, notional, reference_price)?;
    let price = maker_sell_price(precision, reference_price, execution.maker_price_offset_bps);
    let client_order_id = build_short_ladder_client_order_id_at(
        &symbol_cfg.symbol,
        "lm",
        layer_index,
        requested_at.timestamp_millis(),
    );
    Some(short_ladder_order_command(
        ctx,
        exchange_id,
        &account.account_id,
        &symbol_cfg.symbol,
        risk_profile_id,
        client_order_id,
        OrderSide::Sell,
        if execution.use_post_only_entry {
            OrderType::PostOnly
        } else {
            OrderType::Limit
        },
        quantity,
        Some(price),
        Some(if execution.use_post_only_entry {
            TimeInForce::PostOnly
        } else {
            TimeInForce::GoodTilCanceled
        }),
        false,
        requested_at,
        BTreeMap::from([
            ("source_plan".to_string(), json!("short_ladder_entry")),
            ("layer_index".to_string(), json!(layer_index)),
            (
                "position_side".to_string(),
                json!(account.dual_position_mode.then_some("SHORT")),
            ),
        ]),
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn short_ladder_market_entry_to_execution_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    account: &AccountConfig,
    symbol_cfg: &SymbolConfig,
    precision: &SymbolPrecision,
    risk_profile_id: &str,
    notional: f64,
    reference_price: f64,
    requested_at: DateTime<Utc>,
) -> Option<ExecutionOrderCommand> {
    let quantity = derive_order_quantity_from_precision(precision, notional, reference_price)?;
    let client_order_id = build_short_ladder_client_order_id_at(
        &symbol_cfg.symbol,
        "m",
        0,
        requested_at.timestamp_millis(),
    );
    Some(short_ladder_order_command(
        ctx,
        exchange_id,
        &account.account_id,
        &symbol_cfg.symbol,
        risk_profile_id,
        client_order_id,
        OrderSide::Sell,
        OrderType::Market,
        quantity,
        None,
        None,
        false,
        requested_at,
        BTreeMap::from([
            (
                "source_plan".to_string(),
                json!("short_ladder_market_entry"),
            ),
            (
                "position_side".to_string(),
                json!(account.dual_position_mode.then_some("SHORT")),
            ),
        ]),
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn short_ladder_exit_to_execution_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    account: &AccountConfig,
    symbol: &str,
    execution: &ExecutionConfig,
    precision: &SymbolPrecision,
    risk_profile_id: &str,
    position: &LiveShortPosition,
    reference_price: f64,
    reason: &str,
    requested_at: DateTime<Utc>,
) -> Option<ExecutionOrderCommand> {
    let quantity = precision_round_down(position.quantity.abs(), precision.step_size);
    if quantity <= 0.0 {
        return None;
    }
    let maker_price = maker_buy_price(precision, reference_price, execution.maker_price_offset_bps);
    let client_order_id =
        build_short_ladder_client_order_id_at(symbol, "close", 0, requested_at.timestamp_millis());
    Some(short_ladder_order_command(
        ctx,
        exchange_id,
        &account.account_id,
        symbol,
        risk_profile_id,
        client_order_id,
        OrderSide::Buy,
        if execution.close_with_market_order {
            OrderType::Market
        } else {
            OrderType::PostOnly
        },
        quantity,
        (!execution.close_with_market_order).then_some(maker_price),
        (!execution.close_with_market_order).then_some(TimeInForce::PostOnly),
        true,
        requested_at,
        BTreeMap::from([
            ("source_plan".to_string(), json!("short_ladder_exit")),
            ("exit_reason".to_string(), json!(reason)),
            (
                "position_side".to_string(),
                json!(account.dual_position_mode.then_some("SHORT")),
            ),
        ]),
    ))
}

pub fn short_ladder_pending_entry_cancel_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    pending: &PendingEntryOrder,
    requested_at: DateTime<Utc>,
) -> ExecutionCancelCommand {
    short_ladder_cancel_command_from_ids(
        ctx,
        exchange_id,
        symbol,
        risk_profile_id,
        None,
        Some(pending.order_id.clone()),
        &pending.reason,
        requested_at,
        "pending_entry",
    )
}

pub fn short_ladder_pending_initial_cancel_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    pending: &PendingInitialEntry,
    reason: &str,
    requested_at: DateTime<Utc>,
) -> ExecutionCancelCommand {
    short_ladder_cancel_command_from_ids(
        ctx,
        exchange_id,
        symbol,
        risk_profile_id,
        pending.client_order_id.clone(),
        Some(pending.order_id.clone()),
        reason,
        requested_at,
        "pending_initial_entry",
    )
}

pub fn short_ladder_pending_exit_cancel_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    pending: &PendingExitOrder,
    requested_at: DateTime<Utc>,
) -> ExecutionCancelCommand {
    short_ladder_cancel_command_from_ids(
        ctx,
        exchange_id,
        symbol,
        risk_profile_id,
        pending.client_order_id.clone(),
        Some(pending.order_id.clone()),
        &pending.reason,
        requested_at,
        "pending_exit",
    )
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
                "exchange_id": { "type": "string", "minLength": 1, "default": "binance" },
                "market_type": {
                    "type": "string",
                    "enum": ["spot", "margin", "perpetual", "futures"],
                    "default": "perpetual"
                },
                "candle_intervals": {
                    "type": "array",
                    "items": { "type": "string", "minLength": 1 },
                    "default": ["5m"]
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
            "market_data_subscriptions": { "type": "array" },
            "last_timer_at": { "type": ["string", "null"], "format": "date-time" },
            "last_event_digest": { "type": ["object", "null"] },
            "task_signals": { "type": "array" }
        }
    })
}

pub fn runtime_event_digest(event: &StrategyEvent) -> RuntimeEventDigest {
    match event {
        StrategyEvent::Started(event) => RuntimeEventDigest {
            event_kind: "started".to_string(),
            symbol: None,
            account_id: None,
            client_order_id: None,
            command_kind: None,
            timer_id: None,
            observed_at: event.occurred_at,
        },
        StrategyEvent::Stopping(event) => RuntimeEventDigest {
            event_kind: "stopping".to_string(),
            symbol: None,
            account_id: None,
            client_order_id: None,
            command_kind: None,
            timer_id: None,
            observed_at: event.occurred_at,
        },
        StrategyEvent::Execution(event) => RuntimeEventDigest {
            event_kind: "execution".to_string(),
            symbol: event
                .payload
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string),
            account_id: None,
            client_order_id: event.client_order_id.clone(),
            command_kind: None,
            timer_id: None,
            observed_at: event.occurred_at,
        },
        StrategyEvent::MarketData(event) => RuntimeEventDigest {
            event_kind: "market_data".to_string(),
            symbol: Some(event.symbol.clone()),
            account_id: None,
            client_order_id: None,
            command_kind: None,
            timer_id: None,
            observed_at: event.received_at,
        },
        StrategyEvent::Account(event) => RuntimeEventDigest {
            event_kind: "account".to_string(),
            symbol: event
                .payload
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string),
            account_id: Some(event.account_id.clone()),
            client_order_id: None,
            command_kind: None,
            timer_id: None,
            observed_at: event.received_at,
        },
        StrategyEvent::OperatorCommand(command) => RuntimeEventDigest {
            event_kind: "operator_command".to_string(),
            symbol: command
                .payload
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string),
            account_id: None,
            client_order_id: None,
            command_kind: Some(command.command_kind.clone()),
            timer_id: None,
            observed_at: command.requested_at,
        },
        StrategyEvent::Timer(event) => RuntimeEventDigest {
            event_kind: "timer".to_string(),
            symbol: event
                .payload
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string),
            account_id: None,
            client_order_id: None,
            command_kind: None,
            timer_id: Some(event.timer_id.clone()),
            observed_at: event.fired_at,
        },
    }
}

pub fn runtime_task_signals(event: &StrategyEvent) -> Vec<RuntimeTaskSignal> {
    match event {
        StrategyEvent::Timer(event) => vec![RuntimeTaskSignal {
            task_kind: match event.timer_id.as_str() {
                "market" | "market_tick" | "refresh_signal" => "refresh_signal",
                "account" | "account_sync" | "sync_positions" => "sync_account_state",
                "status" | "status_report" => "publish_status",
                other => other,
            }
            .to_string(),
            requested_at: event.fired_at,
            payload: event.payload.clone(),
        }],
        StrategyEvent::OperatorCommand(command)
            if matches!(command.command_kind.as_str(), "refresh_signal" | "resume") =>
        {
            vec![RuntimeTaskSignal {
                task_kind: command.command_kind.clone(),
                requested_at: command.requested_at,
                payload: command.payload.clone(),
            }]
        }
        _ => Vec::new(),
    }
}

pub fn runtime_health_issues(
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

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    ["pause", "resume", "stop", "refresh_signal"]
        .into_iter()
        .map(|command_kind| StrategyCommandSchema {
            command_kind: command_kind.to_string(),
            description: Some(format!("Short ladder live runtime {command_kind} command")),
            payload_schema: json!({
                "type": "object",
                "additionalProperties": true
            }),
        })
        .collect()
}

fn default_exchange_id() -> String {
    "binance".to_string()
}

fn default_market_type() -> MarketType {
    MarketType::Perpetual
}

fn default_short_ladder_candle_intervals() -> Vec<String> {
    vec!["5m".to_string()]
}

#[allow(clippy::too_many_arguments)]
fn short_ladder_order_command(
    ctx: &StrategyContext,
    exchange_id: &str,
    account_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    client_order_id: String,
    side: OrderSide,
    order_type: OrderType,
    quantity: f64,
    price: Option<f64>,
    time_in_force: Option<TimeInForce>,
    reduce_only: bool,
    requested_at: DateTime<Utc>,
    mut metadata: BTreeMap<String, Value>,
) -> ExecutionOrderCommand {
    metadata.insert("strategy_kind".to_string(), json!(STRATEGY_KIND));
    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: account_id.to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: client_order_id.clone(),
        idempotency_key: execution_idempotency_key(ctx, &client_order_id),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: exchange_id.to_string(),
        symbol: symbol.to_string(),
        side,
        order_type,
        quantity: quantity.to_string(),
        price: price.map(|price| price.to_string()),
        time_in_force,
        reduce_only,
        metadata,
    }
}

#[allow(clippy::too_many_arguments)]
fn short_ladder_cancel_command_from_ids(
    ctx: &StrategyContext,
    exchange_id: &str,
    symbol: &str,
    risk_profile_id: &str,
    client_order_id: Option<String>,
    execution_order_id: Option<String>,
    reason: &str,
    requested_at: DateTime<Utc>,
    source: &str,
) -> ExecutionCancelCommand {
    let key_source = client_order_id
        .as_deref()
        .or(execution_order_id.as_deref())
        .unwrap_or(reason)
        .to_string();
    ExecutionCancelCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id,
        execution_order_id,
        idempotency_key: execution_idempotency_key(ctx, &key_source),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: exchange_id.to_string(),
        symbol: symbol.to_string(),
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            ("source_pending_order".to_string(), json!(source)),
            ("cancel_reason".to_string(), json!(reason)),
        ]),
    }
}

fn execution_idempotency_key(ctx: &StrategyContext, client_order_id: &str) -> String {
    format!("{}:{}:{}", ctx.strategy_id(), ctx.run_id(), client_order_id)
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
        ExecutionIntentAck, ExecutionOrderAck, ExecutionOrderCommand, MarketDataEvent, MarketType,
        OrderSide, OrderType, SdkResult, StrategyCommand, StrategyExecutionClient, TimeInForce,
        TimerEvent,
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
            .any(|command| command.command_kind == "refresh_signal"));
        assert_eq!(spec.snapshot_schema.schema_version, 1);
        assert_eq!(spec.snapshot_schema.json_schema["type"], json!("object"));
        assert!(spec.snapshot_schema.json_schema["properties"]["handled_events"].is_object());
        assert!(spec.snapshot_schema.json_schema["properties"]["market_data_events"].is_object());
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
                "symbols": ["ENAUSDC"],
                "initial_notional": "200",
                "max_notional": "2000"
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
        let mut runtime = ShortLadderLiveRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({
                "symbols": ["ENAUSDC", "BTCUSDC"],
                "exchange_id": "paper",
                "market_type": "perpetual",
                "candle_intervals": ["5m"],
                "initial_notional": "200",
                "max_notional": "2000"
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        assert_eq!(runtime.market_data_subscriptions.len(), 2);
        assert_eq!(runtime.market_data_subscriptions[0].exchange_id, "paper");
        assert_eq!(
            runtime.market_data_subscriptions[0].market_type,
            MarketType::Perpetual
        );
        assert!(runtime.market_data_subscriptions[0]
            .channels
            .contains(&MarketDataChannel::OrderBookTop));

        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap();
        runtime
            .handle_event(StrategyEvent::MarketData(MarketDataEvent {
                schema_version: 1,
                exchange_id: "paper".to_string(),
                symbol: "ENAUSDC".to_string(),
                received_at: observed_at,
                payload: json!({"close": "1.0"}),
            }))
            .await
            .expect("market event should apply");
        runtime
            .handle_event(StrategyEvent::Execution(ExecutionEvent {
                schema_version: 1,
                event_id: "exec-1".to_string(),
                client_order_id: Some("sll-ena-1".to_string()),
                occurred_at: observed_at,
                payload: json!({"status": "filled"}),
            }))
            .await
            .expect("execution event should apply");
        runtime
            .handle_event(StrategyEvent::Account(AccountEvent {
                schema_version: 1,
                account_id: "account-1".to_string(),
                received_at: observed_at,
                payload: json!({"positions": []}),
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
        assert_eq!(snapshot.payload["market_data_events"], json!(1));
        assert_eq!(snapshot.payload["execution_events"], json!(1));
        assert_eq!(snapshot.payload["account_events"], json!(1));
        assert_eq!(snapshot.payload["operator_commands"], json!(1));
        assert_eq!(snapshot.payload["configured_symbols"][0], json!("ENAUSDC"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_should_digest_timer_tasks_and_report_stale_health() {
        let mut runtime = ShortLadderLiveRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "short-ladder",
            "run-1",
            json!({"symbols": ["ENAUSDC"], "initial_notional": "200", "max_notional": "2000"}),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        let fired_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 2, 0).unwrap();
        runtime
            .handle_event(StrategyEvent::Timer(TimerEvent {
                schema_version: 1,
                timer_id: "market_tick".to_string(),
                fired_at,
                payload: json!({"symbol": "ENAUSDC"}),
            }))
            .await
            .expect("timer event should apply");

        let snapshot = runtime.snapshot().await.expect("snapshot should build");
        assert_eq!(snapshot.payload["timer_events"], json!(1));
        assert_eq!(
            snapshot.payload["last_event_digest"]["event_kind"],
            json!("timer")
        );
        assert_eq!(
            snapshot.payload["task_signals"][0]["task_kind"],
            json!("refresh_signal")
        );

        let stale = runtime_health_issues(
            fired_at + chrono::Duration::seconds(901),
            &StrategyStatus::Running,
            Some(fired_at),
            None,
            None,
        );
        assert_eq!(stale.len(), 2);
        assert_eq!(stale[0].severity, HealthSeverity::Warning);
        assert_eq!(
            stale[0].details.as_ref().unwrap()["issue_kind"],
            json!("market_data_stale")
        );
    }

    #[test]
    fn ladder_order_helpers_should_map_to_sdk_execution_commands() {
        let execution_client: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-ctx",
            "short-ladder",
            "run-1",
            json!({"symbols": ["ENAUSDC"], "initial_notional": "200", "max_notional": "2000"}),
            execution_client,
        );
        let requested_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 2, 0).unwrap();
        let account = AccountConfig {
            account_id: "account-short".to_string(),
            market_type: MarketType::Perpetual,
            dual_position_mode: true,
            default_leverage: Some(2),
        };
        let symbol_cfg = SymbolConfig {
            symbol: "ENAUSDC".to_string(),
            enabled: true,
            initial_notional: 200.0,
            max_notional: 2000.0,
            leverage: Some(2),
        };
        let precision = SymbolPrecision {
            step_size: 0.01,
            tick_size: 0.001,
            min_notional: 5.0,
            min_order_size: 0.02,
        };
        let execution = ExecutionConfig {
            use_post_only_entry: true,
            close_with_market_order: false,
            ..ExecutionConfig::default()
        };

        let maker_entry = short_ladder_entry_to_execution_command(
            &ctx,
            "paper",
            &account,
            &symbol_cfg,
            &execution,
            &precision,
            "risk-short",
            1,
            20.0,
            1.0,
            requested_at,
        )
        .expect("maker entry command");

        assert_eq!(maker_entry.account_id, "account-short");
        assert_eq!(maker_entry.side, OrderSide::Sell);
        assert_eq!(maker_entry.order_type, OrderType::PostOnly);
        assert_eq!(maker_entry.time_in_force, Some(TimeInForce::PostOnly));
        assert!(!maker_entry.reduce_only);
        assert_eq!(maker_entry.metadata["position_side"], json!("SHORT"));
        assert_eq!(
            maker_entry.metadata["source_plan"],
            json!("short_ladder_entry")
        );

        let market_entry = short_ladder_market_entry_to_execution_command(
            &ctx,
            "paper",
            &account,
            &symbol_cfg,
            &precision,
            "risk-short",
            20.0,
            1.0,
            requested_at,
        )
        .expect("market entry command");
        assert_eq!(market_entry.order_type, OrderType::Market);
        assert_eq!(market_entry.price, None);
        assert_eq!(market_entry.time_in_force, None);

        let position = LiveShortPosition {
            average_entry_price: 1.0,
            quantity: 20.0,
            current_notional: 20.0,
            filled_layers: 1,
            next_layer_index: 1,
            last_layer_price: 1.0,
            atr_at_entry: 0.1,
            breakeven_armed: false,
            trailing_take_profit_armed: false,
            best_favorable_price: None,
            opened_at: requested_at,
            last_sync_at: requested_at,
        };
        let exit = short_ladder_exit_to_execution_command(
            &ctx,
            "paper",
            &account,
            "ENAUSDC",
            &execution,
            &precision,
            "risk-short",
            &position,
            0.9,
            "take_profit",
            requested_at,
        )
        .expect("exit command");
        assert_eq!(exit.side, OrderSide::Buy);
        assert_eq!(exit.order_type, OrderType::PostOnly);
        assert!(exit.reduce_only);
        assert_eq!(exit.metadata["exit_reason"], json!("take_profit"));

        let pending = PendingExitOrder {
            order_id: "venue-exit-1".to_string(),
            client_order_id: Some("sll_ena_close_1".to_string()),
            reason: "replace_exit".to_string(),
            submitted_at: requested_at,
            reference_price: 0.9,
        };
        let cancel = short_ladder_pending_exit_cancel_command(
            &ctx,
            "paper",
            "ENAUSDC",
            "risk-short",
            &pending,
            requested_at,
        );
        assert_eq!(cancel.client_order_id, Some("sll_ena_close_1".to_string()));
        assert_eq!(cancel.execution_order_id, Some("venue-exit-1".to_string()));
        assert_eq!(
            cancel.metadata["source_pending_order"],
            json!("pending_exit")
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
        assert_eq!(config.notifications.channel_id, None);
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
