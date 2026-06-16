use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, ExecutionIntent, MarketDataChannel, MarketDataSubscription, MarketType,
    RequiredAccountPermission, RiskCapability, RiskCapabilityDeclaration, StrategyCommand,
    StrategyCommandSchema, StrategyConfigSchema, StrategyContext, StrategyEvent,
    StrategyExecutionClient, StrategyInstanceId, StrategyRuntime, StrategySnapshot,
    StrategySnapshotSchema, StrategySpec, StrategyStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod core;

pub use core::{
    build_route_position_snapshot, build_split_plan, evaluate_hedge_residual,
    evaluate_route_opportunity, validate_manual_close, validate_manual_open, AccountConfig,
    AlertConfig, CanonicalSymbol, CloseScope, ConfigError, ControlConfig, DefaultsConfig,
    ExecutionConfig, ExecutionStyle, FeeRates, FundingConfig, FundingSnapshot, HedgeEvaluation,
    HedgeStatus, HedgeToleranceConfig, InstrumentKey, LegConfig, LegPositionSnapshot,
    LeverageConfig, ManualCloseCommand, ManualCommandValidation, ManualOpenCommand, MarketConfig,
    OrderBookTop, OrderSide, PersistenceConfig, PositionSide, PositionSidePolicy, RiskConfig,
    RouteConfig, RouteKind, RouteMarketInput, RouteOpportunity, RoutePositionSnapshot,
    SettlementConfig, SizingConfig, SplitExecutionConfig, SplitPlan, ThresholdConfig,
    UnifiedArbitrageConfig,
};

pub const STRATEGY_KIND: &str = "unified_arbitrage";
pub const DISPLAY_NAME: &str = "Unified Arbitrage";
pub const MIGRATED_FROM: &str = "merged:spot_perp,perp_perp,funding_edge,settlement_window";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnifiedArbitrageSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub configured_routes: usize,
    pub active_routes: usize,
    pub active_symbols: Vec<String>,
    pub market_data_subscriptions: Vec<MarketDataSubscription>,
    pub open_routes: usize,
    pub open_bundles: usize,
    pub position_value_usdt: f64,
    pub gross_exposure_usdt: f64,
    pub net_exposure_usdt: f64,
    pub unrealized_pnl_usdt: f64,
    pub realized_pnl_usdt_today: f64,
    pub funding_pnl_usdt_today: f64,
    pub fee_usdt_today: f64,
    pub nearest_liquidation_distance_pct: Option<f64>,
    pub routes_in_close_only: usize,
    pub routes_requiring_repair: usize,
    pub rejected_control_commands: u64,
    pub submitted_execution_intents: u64,
    pub last_opportunities: Vec<RouteOpportunity>,
    pub route_positions: Vec<RoutePositionSnapshot>,
}

pub struct UnifiedArbitrageRuntime {
    instance_id: StrategyInstanceId,
    tenant_id: String,
    account_id: String,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
    config: UnifiedArbitrageConfig,
    market_data_subscriptions: Vec<MarketDataSubscription>,
    execution: Option<Arc<dyn StrategyExecutionClient>>,
    last_opportunities: BTreeMap<String, RouteOpportunity>,
    route_positions: BTreeMap<String, RoutePositionSnapshot>,
    rejected_control_commands: u64,
    submitted_execution_intents: u64,
}

impl UnifiedArbitrageRuntime {
    pub fn new() -> Self {
        Self {
            instance_id: StrategyInstanceId::new("unstarted"),
            tenant_id: "local".to_string(),
            account_id: "unified_arbitrage".to_string(),
            strategy_id: STRATEGY_KIND.to_string(),
            run_id: "unstarted".to_string(),
            status: StrategyStatus::Stopped,
            started_at: None,
            last_event_at: None,
            handled_events: 0,
            config: UnifiedArbitrageConfig::default(),
            market_data_subscriptions: Vec::new(),
            execution: None,
            last_opportunities: BTreeMap::new(),
            route_positions: BTreeMap::new(),
            rejected_control_commands: 0,
            submitted_execution_intents: 0,
        }
    }

    pub fn reload_config_value(&mut self, value: &Value) -> anyhow::Result<()> {
        let config: UnifiedArbitrageConfig = serde_json::from_value(value.clone())?;
        config.validate()?;
        self.market_data_subscriptions = unified_market_data_subscriptions(&config);
        self.config = config;
        Ok(())
    }

    fn snapshot_payload(&self) -> UnifiedArbitrageSnapshotPayload {
        let route_positions = self
            .route_positions
            .values()
            .cloned()
            .collect::<Vec<RoutePositionSnapshot>>();
        let position_value_usdt = route_positions
            .iter()
            .map(|route| route.position_value_usdt)
            .sum::<f64>();
        let gross_exposure_usdt = route_positions
            .iter()
            .flat_map(|route| route.legs.iter())
            .map(|leg| leg.position_value_usdt.abs())
            .sum::<f64>();
        let net_exposure_usdt = route_positions
            .iter()
            .flat_map(|route| route.legs.iter())
            .map(|leg| match leg.position_side {
                PositionSide::Long => leg.position_value_usdt,
                PositionSide::Short => -leg.position_value_usdt,
                PositionSide::None => 0.0,
            })
            .sum::<f64>();
        let unrealized_pnl_usdt = route_positions
            .iter()
            .map(|route| route.unrealized_pnl_usdt)
            .sum::<f64>();
        let realized_pnl_usdt_today = route_positions
            .iter()
            .map(|route| route.realized_pnl_usdt)
            .sum::<f64>();
        let funding_pnl_usdt_today = route_positions
            .iter()
            .map(|route| route.funding_pnl_usdt)
            .sum::<f64>();
        let fee_usdt_today = route_positions
            .iter()
            .map(|route| route.fee_pnl_usdt)
            .sum::<f64>();
        let nearest_liquidation_distance_pct = route_positions
            .iter()
            .filter_map(|route| route.nearest_liquidation_distance_pct)
            .min_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        let routes_requiring_repair = route_positions
            .iter()
            .filter(|route| matches!(route.hedge.status, HedgeStatus::Repairing))
            .count();
        let routes_in_close_only = route_positions
            .iter()
            .filter(|route| {
                matches!(
                    route.hedge.status,
                    HedgeStatus::RouteCloseOnly | HedgeStatus::EmergencyCloseRequired
                )
            })
            .count();

        UnifiedArbitrageSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
            configured_routes: self.config.routes.len(),
            active_routes: self.config.active_routes().len(),
            active_symbols: self.config.active_symbols(),
            market_data_subscriptions: self.market_data_subscriptions.clone(),
            open_routes: route_positions
                .iter()
                .filter(|route| route.position_value_usdt > 0.0)
                .count(),
            open_bundles: route_positions
                .iter()
                .filter(|route| route.position_value_usdt > 0.0)
                .count(),
            position_value_usdt,
            gross_exposure_usdt,
            net_exposure_usdt,
            unrealized_pnl_usdt,
            realized_pnl_usdt_today,
            funding_pnl_usdt_today,
            fee_usdt_today,
            nearest_liquidation_distance_pct,
            routes_in_close_only,
            routes_requiring_repair,
            rejected_control_commands: self.rejected_control_commands,
            submitted_execution_intents: self.submitted_execution_intents,
            last_opportunities: self.last_opportunities.values().cloned().collect(),
            route_positions,
        }
    }

    fn update_market_data(&mut self, payload: Value) -> anyhow::Result<()> {
        if let Ok(input) = serde_json::from_value::<RouteMarketInput>(payload.clone()) {
            self.evaluate_market_input(input);
            return Ok(());
        }
        if let Some(inputs) = payload.get("route_market_inputs") {
            for input in serde_json::from_value::<Vec<RouteMarketInput>>(inputs.clone())? {
                self.evaluate_market_input(input);
            }
        }
        Ok(())
    }

    fn evaluate_market_input(&mut self, input: RouteMarketInput) {
        let Some(route) = self
            .config
            .routes
            .iter()
            .find(|route| route.route_id == input.route_id)
        else {
            return;
        };
        let opportunity = evaluate_route_opportunity(route, &self.config.defaults, &input);
        self.last_opportunities
            .insert(opportunity.route_id.clone(), opportunity);
    }

    fn update_account_snapshot(&mut self, payload: Value) -> anyhow::Result<()> {
        if let Ok(snapshot) = serde_json::from_value::<RoutePositionSnapshot>(payload.clone()) {
            self.route_positions
                .insert(snapshot.route_id.clone(), snapshot);
            return Ok(());
        }
        if let Some(snapshots) = payload.get("route_positions") {
            for snapshot in serde_json::from_value::<Vec<RoutePositionSnapshot>>(snapshots.clone())?
            {
                self.route_positions
                    .insert(snapshot.route_id.clone(), snapshot);
            }
        }
        Ok(())
    }

    async fn handle_operator_command(&mut self, command: StrategyCommand) -> anyhow::Result<()> {
        match command.command_kind.as_str() {
            "manual_open_route" => {
                let open = serde_json::from_value::<ManualOpenCommand>(command.payload.clone())?;
                let current_signal = self.last_opportunities.get(&open.route_id);
                let validation = validate_manual_open(&open, &self.config, current_signal);
                if !validation.accepted {
                    self.rejected_control_commands += 1;
                    return Ok(());
                }
                let Some(route) = self
                    .config
                    .routes
                    .iter()
                    .find(|route| route.route_id == open.route_id)
                    .cloned()
                else {
                    self.rejected_control_commands += 1;
                    return Ok(());
                };
                let split_plan = if open.use_split_execution {
                    build_split_plan(
                        open.notional_usdt,
                        route.split_config(&self.config.defaults),
                    )
                } else {
                    build_split_plan(
                        open.notional_usdt,
                        SplitExecutionConfig {
                            enabled: false,
                            ..route.split_config(&self.config.defaults)
                        },
                    )
                };
                let route_id = open.route_id.clone();
                let payload = json!({
                    "command_id": command.command_id,
                    "route_id": route_id,
                    "symbol": route.symbol,
                    "route_kind": route.kind.to_string(),
                    "notional_usdt": open.notional_usdt,
                    "execution_style": open.execution_style.unwrap_or(route.execution_config(&self.config.defaults).preferred_open_style),
                    "max_slippage_bps": open.max_slippage_bps,
                    "split_plan": split_plan,
                    "legs": route.legs,
                    "owned_position_scope": {
                        "strategy_id": self.strategy_id,
                        "run_id": self.run_id,
                        "route_id": route_id
                    }
                });
                self.submit_execution_intent("unified_arbitrage_open_route", payload)
                    .await?;
            }
            "manual_close" => {
                let close = serde_json::from_value::<ManualCloseCommand>(command.payload.clone())?;
                let validation = validate_manual_close(&close, &self.config);
                if !validation.accepted {
                    self.rejected_control_commands += 1;
                    return Ok(());
                }
                let payload = json!({
                    "command_id": command.command_id,
                    "scope": close.scope,
                    "route_id": close.route_id,
                    "bundle_id": close.bundle_id,
                    "symbol": close.symbol,
                    "reason": close.reason,
                    "execution_style": close.execution_style.unwrap_or(ExecutionStyle::DualTakerReduceOnly),
                    "reduce_only": close.reduce_only,
                    "close_residual_only": close.close_residual_only,
                    "max_slippage_bps": close.max_slippage_bps,
                    "owned_position_scope": {
                        "strategy_id": self.strategy_id,
                        "run_id": self.run_id
                    }
                });
                self.submit_execution_intent("unified_arbitrage_close", payload)
                    .await?;
            }
            "pause_new_entries" | "set_route_close_only" => self.status = StrategyStatus::Degraded,
            "resume_new_entries" | "clear_route_close_only" => {
                self.status = StrategyStatus::Running
            }
            "stop" => self.status = StrategyStatus::Stopping,
            _ => {}
        }
        Ok(())
    }

    async fn submit_execution_intent(
        &mut self,
        intent_kind: &str,
        payload: Value,
    ) -> anyhow::Result<()> {
        let Some(execution) = self.execution.as_ref().map(Arc::clone) else {
            self.rejected_control_commands += 1;
            return Ok(());
        };
        let idempotency_key = payload
            .get("command_id")
            .and_then(Value::as_str)
            .map(|command_id| format!("{}:{}:{command_id}", self.strategy_id, self.run_id))
            .unwrap_or_else(|| {
                format!(
                    "{}:{}:{}:{}",
                    self.strategy_id,
                    self.run_id,
                    intent_kind,
                    Utc::now().timestamp_millis()
                )
            });
        execution
            .submit_raw_intent(ExecutionIntent {
                schema_version: 1,
                intent_kind: intent_kind.to_string(),
                tenant_id: self.tenant_id.clone(),
                account_id: self.account_id.clone(),
                strategy_id: self.strategy_id.clone(),
                run_id: self.run_id.clone(),
                idempotency_key,
                requested_at: Utc::now(),
                payload,
            })
            .await?;
        self.submitted_execution_intents += 1;
        Ok(())
    }
}

impl Default for UnifiedArbitrageRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for UnifiedArbitrageRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        let config: UnifiedArbitrageConfig = serde_json::from_value(ctx.config().clone())?;
        config.validate()?;
        self.instance_id = ctx.instance_id().clone();
        self.tenant_id = ctx.tenant_id().to_string();
        self.account_id = ctx.account_id().to_string();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.last_event_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.market_data_subscriptions = unified_market_data_subscriptions(&config);
        self.config = config;
        self.execution = Some(ctx.execution());
        self.handled_events = 0;
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.status = StrategyStatus::Stopped;
        Ok(())
    }

    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()> {
        self.handled_events += 1;
        self.last_event_at = Some(event_timestamp(&event));
        match event {
            StrategyEvent::Started(_) => self.status = StrategyStatus::Running,
            StrategyEvent::Stopping(_) => self.status = StrategyStatus::Stopping,
            StrategyEvent::MarketData(event) => self.update_market_data(event.payload)?,
            StrategyEvent::Account(event) => self.update_account_snapshot(event.payload)?,
            StrategyEvent::OperatorCommand(command) => {
                self.handle_operator_command(command).await?;
            }
            _ => {}
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

pub fn unified_market_data_subscriptions(
    config: &UnifiedArbitrageConfig,
) -> Vec<MarketDataSubscription> {
    let mut subscriptions = Vec::new();
    for route in config.active_routes() {
        for leg in route.legs.values() {
            let mut channels = vec![
                MarketDataChannel::OrderBookTop,
                MarketDataChannel::OrderBookDepth,
            ];
            if leg.market_type == MarketType::Perpetual {
                channels.push(MarketDataChannel::FundingRate);
                channels.push(MarketDataChannel::MarkPrice);
                channels.push(MarketDataChannel::IndexPrice);
            }
            subscriptions.push(MarketDataSubscription {
                exchange_id: leg.exchange.trim().to_ascii_lowercase(),
                symbol: route.symbol.clone(),
                market_type: leg.market_type.clone(),
                channels,
            });
        }
    }
    subscriptions.sort_by(|left, right| {
        (
            &left.exchange_id,
            &left.symbol,
            serde_json::to_string(&left.market_type).unwrap_or_default(),
        )
            .cmp(&(
                &right.exchange_id,
                &right.symbol,
                serde_json::to_string(&right.market_type).unwrap_or_default(),
            ))
    });
    subscriptions.dedup_by(|left, right| {
        left.exchange_id == right.exchange_id
            && left.symbol == right.symbol
            && left.market_type == right.market_type
    });
    subscriptions
}

pub fn strategy_spec() -> StrategySpec {
    StrategySpec {
        schema_version: 1,
        strategy_kind: STRATEGY_KIND.to_string(),
        display_name: DISPLAY_NAME.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        description: Some(
            "Unified route-based spot/perp, perp/perp, and settlement arbitrage strategy"
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: command_schemas(),
        risk_capabilities: vec![
            risk_capability(RiskCapability::PlaceOrders, "Open route legs"),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancel stale maker and repair orders",
            ),
            risk_capability(
                RiskCapability::ReduceOnlyOrders,
                "Close owned route positions",
            ),
            risk_capability(RiskCapability::Hedging, "Repair residual leg imbalance"),
            risk_capability(
                RiskCapability::CrossAccountRead,
                "Read balances, positions, funding, and liquidation risk",
            ),
        ],
        market_data_subscriptions: unified_market_data_subscriptions(
            &UnifiedArbitrageConfig::default(),
        ),
        required_account_permissions: required_permissions(),
        metadata: BTreeMap::from([
            ("migrated_from".to_string(), json!(MIGRATED_FROM)),
            (
                "route_kinds".to_string(),
                json!([
                    "spot_perp_basis",
                    "perp_perp_spread",
                    "single_exchange_settlement"
                ]),
            ),
            ("manual_open_close_reserved".to_string(), json!(true)),
        ]),
    }
}

fn risk_capability(capability: RiskCapability, description: &str) -> RiskCapabilityDeclaration {
    RiskCapabilityDeclaration {
        capability,
        description: Some(description.to_string()),
        limits: json!({}),
    }
}

fn required_permissions() -> Vec<RequiredAccountPermission> {
    vec![
        permission(
            AccountPermission::ReadBalances,
            "Compute account and owned exposure",
        ),
        permission(
            AccountPermission::ReadPositions,
            "Compute PnL and liquidation distance",
        ),
        permission(
            AccountPermission::ReadOrders,
            "Reconcile bundle order state",
        ),
        permission(
            AccountPermission::ReadFills,
            "Attribute fills to owned route positions",
        ),
        permission(
            AccountPermission::TradeSpot,
            "Open and close spot/perp routes",
        ),
        permission(
            AccountPermission::TradePerpetual,
            "Open and close perp legs",
        ),
        permission(
            AccountPermission::CancelOrders,
            "Cancel stale or unsafe orders",
        ),
    ]
}

fn permission(permission: AccountPermission, reason: &str) -> RequiredAccountPermission {
    RequiredAccountPermission {
        account_id: None,
        permission,
        reason: Some(reason.to_string()),
    }
}

pub fn command_schemas() -> Vec<StrategyCommandSchema> {
    vec![
        StrategyCommandSchema {
            command_kind: "manual_open_route".to_string(),
            description: Some(
                "Operator-triggered open routed through normal admission gates".to_string(),
            ),
            payload_schema: json!({
                "type": "object",
                "required": ["route_id", "notional_usdt", "max_slippage_bps"],
                "properties": {
                    "route_id": { "type": "string" },
                    "notional_usdt": { "type": "number", "exclusiveMinimum": 0.0 },
                    "execution_style": { "type": ["string", "null"] },
                    "use_split_execution": { "type": "boolean", "default": true },
                    "max_slippage_bps": { "type": "number", "minimum": 0.0 },
                    "require_current_signal": { "type": "boolean", "default": true },
                    "dry_run_preview": { "type": "boolean", "default": false },
                    "operator_confirmation_id": { "type": ["string", "null"] }
                }
            }),
        },
        StrategyCommandSchema {
            command_kind: "manual_close".to_string(),
            description: Some(
                "Operator-triggered reduce-only close by bundle, route, symbol, or all".to_string(),
            ),
            payload_schema: json!({
                "type": "object",
                "required": ["scope", "reason", "reduce_only", "max_slippage_bps"],
                "properties": {
                    "scope": { "enum": ["bundle", "route", "symbol", "all"] },
                    "route_id": { "type": ["string", "null"] },
                    "bundle_id": { "type": ["string", "null"] },
                    "symbol": { "type": ["string", "null"] },
                    "reason": { "type": "string" },
                    "execution_style": { "type": ["string", "null"] },
                    "reduce_only": { "type": "boolean", "const": true },
                    "close_residual_only": { "type": "boolean", "default": false },
                    "max_slippage_bps": { "type": "number", "minimum": 0.0 },
                    "dry_run_preview": { "type": "boolean", "default": false },
                    "operator_confirmation_id": { "type": ["string", "null"] }
                }
            }),
        },
        simple_command("pause_new_entries"),
        simple_command("resume_new_entries"),
        simple_command("set_route_close_only"),
        simple_command("clear_route_close_only"),
        simple_command("cancel_route_orders"),
        simple_command("repair_route_hedge"),
        simple_command("set_route_limits"),
        simple_command("reload_config"),
    ]
}

fn simple_command(kind: &str) -> StrategyCommandSchema {
    StrategyCommandSchema {
        command_kind: kind.to_string(),
        description: None,
        payload_schema: json!({ "type": "object", "additionalProperties": true }),
    }
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "required": ["strategy_kind", "routes"],
            "properties": {
                "strategy_kind": { "const": "unified_arbitrage" },
                "mode": { "type": "string" },
                "trading_mode": { "type": "string" },
                "enable_live_trading": { "type": "boolean" },
                "account": { "type": "object" },
                "market": { "type": "object" },
                "defaults": { "type": "object" },
                "routes": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["route_id", "kind", "symbol", "legs"],
                        "properties": {
                            "route_id": { "type": "string" },
                            "enabled": { "type": "boolean" },
                            "kind": {
                                "enum": [
                                    "spot_perp_basis",
                                    "perp_perp_spread",
                                    "single_exchange_settlement"
                                ]
                            },
                            "symbol": { "type": "string" },
                            "legs": { "type": "object" }
                        }
                    }
                }
            }
        }),
    }
}

pub fn snapshot_schema() -> StrategySnapshotSchema {
    StrategySnapshotSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "required": [
                "migrated_from",
                "configured_routes",
                "active_routes",
                "position_value_usdt",
                "unrealized_pnl_usdt",
                "nearest_liquidation_distance_pct"
            ],
            "properties": {
                "migrated_from": { "type": "string" },
                "configured_routes": { "type": "integer" },
                "active_routes": { "type": "integer" },
                "active_symbols": { "type": "array", "items": { "type": "string" } },
                "position_value_usdt": { "type": "number" },
                "gross_exposure_usdt": { "type": "number" },
                "net_exposure_usdt": { "type": "number" },
                "unrealized_pnl_usdt": { "type": "number" },
                "realized_pnl_usdt_today": { "type": "number" },
                "funding_pnl_usdt_today": { "type": "number" },
                "fee_usdt_today": { "type": "number" },
                "nearest_liquidation_distance_pct": { "type": ["number", "null"] }
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
        StrategyEvent::OperatorCommand(event) => event.requested_at,
        StrategyEvent::Timer(event) => event.fired_at,
    }
}
