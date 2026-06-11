use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, ExecutionOrderCommand, HealthSeverity, MarketDataChannel,
    MarketDataSubscription, MarketType, OrderSide, OrderType, RequiredAccountPermission,
    RiskCapability, RiskCapabilityDeclaration, StrategyCommandSchema, StrategyConfigSchema,
    StrategyContext, StrategyEvent, StrategyHealthIssue, StrategyInstanceId, StrategyRuntime,
    StrategySnapshot, StrategySnapshotSchema, StrategySpec, StrategyStatus, TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub mod core;
pub mod runtime_contract;

pub use core::{
    apply_spot_futures_bundle_event, default_excluded_bases, evaluate_spot_futures_opportunity,
    normalize_symbols, CanonicalSymbol, FeeRates, FundingSnapshot, InstrumentKey, OrderBookTop,
    PrecisionRegistry, SpotFuturesBundle, SpotFuturesBundleAction, SpotFuturesBundleEvent,
    SpotFuturesBundleStatus, SpotFuturesClosePlan, SpotFuturesClosePricing,
    SpotFuturesExecutionConfig, SpotFuturesFundingConfig, SpotFuturesHedgePlan,
    SpotFuturesMarketType, SpotFuturesOpportunity, SpotFuturesOpportunityAudit,
    SpotFuturesOrderDraft, SpotFuturesOrderRole, SpotFuturesOrderSide, SpotFuturesRejectReason,
    SpotFuturesRoute, SpotFuturesSelectionConfig, SpotFuturesSizingConfig, SymbolPrecision,
};
pub use runtime_contract::{
    build_runtime_contract, default_runtime_contract, RuntimeProviderContract, RuntimeTaskContract,
    SpotFuturesDashboardSnapshot, SpotFuturesDashboardSnapshotProvider,
    SpotFuturesExecutionProvider, SpotFuturesMarketDataProvider, SpotFuturesNotificationProvider,
    SpotFuturesRuntimeContract, SpotFuturesRuntimeMode, SpotFuturesStorageProvider,
};

pub const STRATEGY_KIND: &str = "spot_futures_arbitrage";
pub const DISPLAY_NAME: &str = "Spot Futures Arbitrage";
pub const MIGRATED_FROM: &str = "strategy-design:spot_futures_arbitrage_design_zh";
pub const DEFAULT_RISK_PROFILE_ID: &str = "spot_futures_arbitrage_default";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesMarketConfig {
    pub quote_asset: String,
    pub depth_levels: usize,
    pub stale_quote_ms: u64,
}

impl Default for SpotFuturesMarketConfig {
    fn default() -> Self {
        Self {
            quote_asset: "USDT".to_string(),
            depth_levels: 5,
            stale_quote_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesUniverseConfig {
    pub enabled_spot_exchanges: Vec<String>,
    pub enabled_perp_exchanges: Vec<String>,
    pub symbols: Vec<String>,
    pub excluded_bases: Vec<String>,
    pub excluded_symbols: Vec<String>,
    pub mode: String,
}

impl Default for SpotFuturesUniverseConfig {
    fn default() -> Self {
        Self {
            enabled_spot_exchanges: vec![
                "gate".to_string(),
                "bitget".to_string(),
                "okx".to_string(),
            ],
            enabled_perp_exchanges: vec![
                "gate".to_string(),
                "bitget".to_string(),
                "okx".to_string(),
            ],
            symbols: vec![
                "ORDI/USDT".to_string(),
                "WLD/USDT".to_string(),
                "ARB/USDT".to_string(),
            ],
            excluded_bases: default_excluded_bases(),
            excluded_symbols: Vec::new(),
            mode: "dynamic".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesRiskConfig {
    pub start_paused_new_entries: bool,
    pub start_close_only: bool,
    pub max_open_bundles: usize,
    pub max_active_bundles_per_symbol: usize,
    pub max_notional_per_symbol_usdt: f64,
    pub max_notional_per_exchange_usdt: f64,
    pub max_total_notional_usdt: f64,
    pub max_unhedged_spot_notional_usdt: f64,
    pub max_hold_seconds: i64,
    pub max_daily_loss_usdt: f64,
    pub max_drawdown_usdt: f64,
    pub symbol_whitelist_required_live: bool,
    pub orphan_exposure_blocks_new_entries: bool,
}

impl Default for SpotFuturesRiskConfig {
    fn default() -> Self {
        Self {
            start_paused_new_entries: true,
            start_close_only: false,
            max_open_bundles: 10,
            max_active_bundles_per_symbol: 1,
            max_notional_per_symbol_usdt: 100.0,
            max_notional_per_exchange_usdt: 500.0,
            max_total_notional_usdt: 1000.0,
            max_unhedged_spot_notional_usdt: 10.0,
            max_hold_seconds: 86_400,
            max_daily_loss_usdt: 50.0,
            max_drawdown_usdt: 100.0,
            symbol_whitelist_required_live: true,
            orphan_exposure_blocks_new_entries: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesPersistenceConfig {
    pub enabled: bool,
    pub jsonl_dir: String,
    pub trade_ledger_path: String,
    pub clickhouse_enabled: bool,
    pub stop_live_on_unavailable: bool,
}

impl Default for SpotFuturesPersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            jsonl_dir: "logs/spot_futures_arbitrage".to_string(),
            trade_ledger_path: "logs/spot_futures_arbitrage/trade_events.jsonl".to_string(),
            clickhouse_enabled: false,
            stop_live_on_unavailable: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesDashboardConfig {
    pub enabled: bool,
    pub port: u16,
}

impl Default for SpotFuturesDashboardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8092,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesArbitrageConfig {
    pub mode: String,
    pub trading_mode: String,
    pub enable_live_trading: bool,
    pub market: SpotFuturesMarketConfig,
    pub universe: SpotFuturesUniverseConfig,
    pub selection: SpotFuturesSelectionConfig,
    pub funding: SpotFuturesFundingConfig,
    pub execution: SpotFuturesExecutionConfig,
    pub sizing: SpotFuturesSizingConfig,
    pub risk: SpotFuturesRiskConfig,
    pub persistence: SpotFuturesPersistenceConfig,
    pub dashboard: SpotFuturesDashboardConfig,
}

impl Default for SpotFuturesArbitrageConfig {
    fn default() -> Self {
        Self {
            mode: "observe".to_string(),
            trading_mode: "paper".to_string(),
            enable_live_trading: false,
            market: SpotFuturesMarketConfig::default(),
            universe: SpotFuturesUniverseConfig::default(),
            selection: SpotFuturesSelectionConfig::default(),
            funding: SpotFuturesFundingConfig::default(),
            execution: SpotFuturesExecutionConfig::default(),
            sizing: SpotFuturesSizingConfig::default(),
            risk: SpotFuturesRiskConfig::default(),
            persistence: SpotFuturesPersistenceConfig::default(),
            dashboard: SpotFuturesDashboardConfig::default(),
        }
    }
}

impl SpotFuturesArbitrageConfig {
    pub fn active_symbols(&self) -> Vec<String> {
        let mut symbols = normalize_symbols(&self.universe.symbols, &self.universe.excluded_bases);
        let excluded: Vec<_> = self
            .universe
            .excluded_symbols
            .iter()
            .filter_map(|symbol| CanonicalSymbol::parse(symbol))
            .map(|symbol| symbol.as_pair())
            .collect();
        symbols.retain(|symbol| !excluded.contains(symbol));
        symbols
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SpotFuturesSnapshotPayload {
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
    pub settings: SpotFuturesArbitrageConfig,
    pub configured_spot_exchanges: Vec<String>,
    pub configured_perp_exchanges: Vec<String>,
    pub active_symbols: Vec<String>,
    pub market_data_subscriptions: Vec<MarketDataSubscription>,
    pub runtime_contract: SpotFuturesRuntimeContract,
}

#[derive(Debug, Clone)]
pub struct SpotFuturesArbitrageRuntime {
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
    config: SpotFuturesArbitrageConfig,
    market_data_subscriptions: Vec<MarketDataSubscription>,
}

impl SpotFuturesArbitrageRuntime {
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
            config: SpotFuturesArbitrageConfig::default(),
            market_data_subscriptions: Vec::new(),
        }
    }

    fn snapshot_payload(&self) -> SpotFuturesSnapshotPayload {
        SpotFuturesSnapshotPayload {
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
            settings: self.config.clone(),
            configured_spot_exchanges: self.config.universe.enabled_spot_exchanges.clone(),
            configured_perp_exchanges: self.config.universe.enabled_perp_exchanges.clone(),
            active_symbols: self.config.active_symbols(),
            market_data_subscriptions: self.market_data_subscriptions.clone(),
            runtime_contract: build_runtime_contract(&self.config, Utc::now()),
        }
    }
}

impl Default for SpotFuturesArbitrageRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for SpotFuturesArbitrageRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        let config: SpotFuturesArbitrageConfig = serde_json::from_value(ctx.config().clone())?;
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.last_event_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.market_data_subscriptions = spot_futures_market_data_subscriptions(&config);
        self.config = config;
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
            "Independent Spot + USDT perpetual cash-and-carry arbitrage strategy.".to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places spot maker open and perpetual taker hedge orders",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels stale spot maker orders",
            ),
            risk_capability(
                RiskCapability::ReduceOnlyOrders,
                "Closes perpetual hedges using reduce-only orders",
            ),
            risk_capability(
                RiskCapability::Hedging,
                "Maintains spot/perpetual hedged exposure",
            ),
            risk_capability(
                RiskCapability::CrossAccountRead,
                "Reads balances, positions, orders, fills, and funding state",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Reserves spot quote and perpetual margin before opening",
            ),
        ],
        market_data_subscriptions: spot_futures_market_data_subscriptions(
            &SpotFuturesArbitrageConfig::default(),
        ),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadPositions,
            AccountPermission::ReadOrders,
            AccountPermission::ReadFills,
            AccountPermission::TradeSpot,
            AccountPermission::TradePerpetual,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            (
                "design_doc".to_string(),
                json!("docs/spot_futures_arbitrage_design_zh.md"),
            ),
            ("runtime_contract_migration".to_string(), json!(true)),
            (
                "primary_hedge_direction".to_string(),
                json!("long_spot_short_perp"),
            ),
            (
                "excluded_major_bases".to_string(),
                json!(default_excluded_bases()),
            ),
        ]),
    }
}

pub fn spot_futures_market_data_subscriptions(
    config: &SpotFuturesArbitrageConfig,
) -> Vec<MarketDataSubscription> {
    let symbols = config.active_symbols();
    let mut subscriptions = Vec::new();
    for exchange in &config.universe.enabled_spot_exchanges {
        for symbol in &symbols {
            subscriptions.push(MarketDataSubscription {
                exchange_id: exchange.trim().to_ascii_lowercase(),
                symbol: symbol.clone(),
                market_type: MarketType::Spot,
                channels: vec![
                    MarketDataChannel::OrderBookDepth,
                    MarketDataChannel::OrderBookTop,
                ],
            });
        }
    }
    for exchange in &config.universe.enabled_perp_exchanges {
        for symbol in &symbols {
            subscriptions.push(MarketDataSubscription {
                exchange_id: exchange.trim().to_ascii_lowercase(),
                symbol: symbol.clone(),
                market_type: MarketType::Perpetual,
                channels: vec![
                    MarketDataChannel::OrderBookDepth,
                    MarketDataChannel::OrderBookTop,
                    MarketDataChannel::FundingRate,
                    MarketDataChannel::MarkPrice,
                    MarketDataChannel::IndexPrice,
                ],
            });
        }
    }
    subscriptions
}

pub fn spot_futures_order_draft_to_execution_command(
    ctx: &StrategyContext,
    client_order_id: &str,
    draft: &SpotFuturesOrderDraft,
    requested_at: DateTime<Utc>,
) -> ExecutionOrderCommand {
    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: client_order_id.to_string(),
        idempotency_key: spot_futures_idempotency_key(ctx, client_order_id),
        risk_profile_id: DEFAULT_RISK_PROFILE_ID.to_string(),
        requested_at,
        exchange_id: draft.instrument.exchange.clone(),
        symbol: draft.instrument.canonical_symbol.as_pair(),
        side: match draft.side {
            SpotFuturesOrderSide::Buy => OrderSide::Buy,
            SpotFuturesOrderSide::Sell => OrderSide::Sell,
        },
        order_type: if draft.post_only {
            OrderType::PostOnly
        } else if draft.time_in_force.eq_ignore_ascii_case("ioc") {
            OrderType::ImmediateOrCancel
        } else {
            OrderType::Limit
        },
        quantity: draft.base_quantity.to_string(),
        price: Some(draft.limit_price.to_string()),
        time_in_force: Some(if draft.post_only {
            TimeInForce::PostOnly
        } else if draft.time_in_force.eq_ignore_ascii_case("ioc") {
            TimeInForce::ImmediateOrCancel
        } else if draft.time_in_force.eq_ignore_ascii_case("fok") {
            TimeInForce::FillOrKill
        } else {
            TimeInForce::GoodTilCanceled
        }),
        reduce_only: draft.reduce_only,
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            ("source_plan".to_string(), json!("spot_futures_order_draft")),
            (
                "market_type".to_string(),
                json!(draft.instrument.market_type),
            ),
            ("role".to_string(), json!(draft.role)),
            ("post_only".to_string(), json!(draft.post_only)),
            ("reduce_only".to_string(), json!(draft.reduce_only)),
        ]),
    }
}

fn spot_futures_idempotency_key(ctx: &StrategyContext, client_order_id: &str) -> String {
    format!(
        "{}:{}:{}:{}",
        STRATEGY_KIND,
        ctx.strategy_id(),
        ctx.run_id(),
        client_order_id
    )
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": true,
            "properties": {
                "mode": { "type": "string" },
                "trading_mode": { "type": "string" },
                "enable_live_trading": { "type": "boolean" },
                "market": { "type": "object" },
                "universe": { "type": "object" },
                "selection": { "type": "object" },
                "funding": { "type": "object" },
                "execution": { "type": "object" },
                "sizing": { "type": "object" },
                "risk": { "type": "object" },
                "persistence": { "type": "object" },
                "dashboard": { "type": "object" }
            }
        }),
    }
}

pub fn snapshot_schema() -> StrategySnapshotSchema {
    StrategySnapshotSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": true,
            "required": ["migrated_from", "handled_events", "settings", "active_symbols"],
            "properties": {
                "migrated_from": { "type": "string" },
                "handled_events": { "type": "integer", "minimum": 0 },
                "settings": { "type": "object" },
                "active_symbols": { "type": "array", "items": { "type": "string" } },
                "runtime_contract": { "type": "object" }
            }
        }),
    }
}

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    ["pause", "resume", "stop", "refresh_universe", "close_only"]
        .into_iter()
        .map(|command_kind| StrategyCommandSchema {
            command_kind: command_kind.to_string(),
            description: Some(format!(
                "Spot-futures arbitrage runtime {command_kind} command"
            )),
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
        "spot_futures_market_data_stale",
        "No recent spot-futures market data event observed",
        900,
    );
    push_staleness_issue(
        &mut issues,
        now,
        started_at,
        last_account_sync_at,
        "spot_futures_account_sync_stale",
        "No recent spot/perpetual account sync event observed",
        1800,
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
        limits: json!({}),
    }
}

fn account_permissions(permissions: &[AccountPermission]) -> Vec<RequiredAccountPermission> {
    permissions
        .iter()
        .cloned()
        .map(|permission| RequiredAccountPermission {
            account_id: None,
            permission,
            reason: Some("spot-futures cash-and-carry strategy requirement".to_string()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rustcta_strategy_sdk::{
        ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
        ExecutionOrderAck, SdkResult, StrategyExecutionClient, StrategyInstanceId,
    };
    use serde_json::json;
    use std::sync::Arc;

    struct RejectingExecutionClient;

    #[async_trait]
    impl StrategyExecutionClient for RejectingExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<ExecutionOrderAck> {
            Ok(ExecutionOrderAck {
                schema_version: command.schema_version,
                accepted: false,
                client_order_id: command.client_order_id,
                execution_order_id: None,
                reason: Some("test client".to_string()),
                received_at: Utc::now(),
            })
        }

        async fn cancel_order(
            &self,
            command: ExecutionCancelCommand,
        ) -> SdkResult<ExecutionCancelAck> {
            Ok(ExecutionCancelAck {
                schema_version: command.schema_version,
                accepted: false,
                client_order_id: command.client_order_id,
                execution_order_id: command.execution_order_id,
                reason: Some("test client".to_string()),
                received_at: Utc::now(),
            })
        }

        async fn submit_raw_intent(
            &self,
            intent: ExecutionIntent,
        ) -> SdkResult<ExecutionIntentAck> {
            Ok(ExecutionIntentAck {
                schema_version: intent.schema_version,
                accepted: false,
                intent_kind: intent.intent_kind,
                reason: Some("test client".to_string()),
                received_at: Utc::now(),
                payload: json!({}),
            })
        }
    }

    fn test_context() -> StrategyContext {
        StrategyContext::new(
            StrategyInstanceId::new("spot_futures_arb_live:test"),
            "tenant",
            "account",
            "spot_futures_arb_live",
            "run-1",
            json!({}),
            Arc::new(RejectingExecutionClient),
        )
    }

    #[test]
    fn default_config_should_exclude_major_bases() {
        let config = SpotFuturesArbitrageConfig::default();
        assert!(config.universe.excluded_bases.contains(&"BTC".to_string()));
        assert!(config.universe.excluded_bases.contains(&"ETH".to_string()));
        assert!(config.universe.excluded_bases.contains(&"BNB".to_string()));
        assert!(config.universe.excluded_bases.contains(&"SOL".to_string()));
    }

    #[test]
    fn subscriptions_should_include_spot_perp_books_and_funding() {
        let config: SpotFuturesArbitrageConfig = serde_json::from_value(json!({
            "universe": {
                "enabled_spot_exchanges": ["gate"],
                "enabled_perp_exchanges": ["bitget"],
                "symbols": ["ORDI/USDT"],
                "excluded_bases": ["BTC", "ETH", "BNB", "SOL"],
                "excluded_symbols": [],
                "mode": "static"
            }
        }))
        .unwrap();
        let subscriptions = spot_futures_market_data_subscriptions(&config);
        assert_eq!(subscriptions.len(), 2);
        assert_eq!(subscriptions[0].market_type, MarketType::Spot);
        assert_eq!(subscriptions[1].market_type, MarketType::Perpetual);
        assert!(subscriptions[1]
            .channels
            .contains(&MarketDataChannel::FundingRate));
    }

    #[test]
    fn spot_maker_draft_should_convert_to_post_only_execution_command() {
        let route = SpotFuturesRoute {
            route_id: "gate-bitget-ordi".to_string(),
            canonical_symbol: CanonicalSymbol::parse("ORDI/USDT").unwrap(),
            spot_exchange: "gate".to_string(),
            perp_exchange: "bitget".to_string(),
        };
        let draft = SpotFuturesOrderDraft {
            instrument: route.spot_key(),
            role: SpotFuturesOrderRole::SpotMakerBuy,
            side: SpotFuturesOrderSide::Buy,
            base_quantity: 2.5,
            limit_price: 9.99,
            post_only: true,
            reduce_only: false,
            time_in_force: "gtc".to_string(),
        };
        let command = spot_futures_order_draft_to_execution_command(
            &test_context(),
            "sf-open-1",
            &draft,
            Utc::now(),
        );

        assert_eq!(command.exchange_id, "gate");
        assert_eq!(command.symbol, "ORDI/USDT");
        assert_eq!(command.side, OrderSide::Buy);
        assert_eq!(command.order_type, OrderType::PostOnly);
        assert_eq!(command.time_in_force, Some(TimeInForce::PostOnly));
        assert!(!command.reduce_only);
        assert_eq!(command.metadata["role"], json!("spot_maker_buy"));
    }

    #[test]
    fn hedge_and_close_drafts_should_convert_to_ioc_and_reduce_only_when_required() {
        let route = SpotFuturesRoute {
            route_id: "gate-bitget-ordi".to_string(),
            canonical_symbol: CanonicalSymbol::parse("ORDI/USDT").unwrap(),
            spot_exchange: "gate".to_string(),
            perp_exchange: "bitget".to_string(),
        };
        let hedge = SpotFuturesOrderDraft {
            instrument: route.perp_key(),
            role: SpotFuturesOrderRole::PerpTakerShortHedge,
            side: SpotFuturesOrderSide::Sell,
            base_quantity: 2.5,
            limit_price: 10.1,
            post_only: false,
            reduce_only: false,
            time_in_force: "ioc".to_string(),
        };
        let hedge_command = spot_futures_order_draft_to_execution_command(
            &test_context(),
            "sf-hedge-1",
            &hedge,
            Utc::now(),
        );
        assert_eq!(hedge_command.exchange_id, "bitget");
        assert_eq!(hedge_command.side, OrderSide::Sell);
        assert_eq!(hedge_command.order_type, OrderType::ImmediateOrCancel);
        assert_eq!(
            hedge_command.time_in_force,
            Some(TimeInForce::ImmediateOrCancel)
        );
        assert!(!hedge_command.reduce_only);

        let close = SpotFuturesOrderDraft {
            role: SpotFuturesOrderRole::PerpReduceOnlyCloseBuy,
            side: SpotFuturesOrderSide::Buy,
            reduce_only: true,
            ..hedge
        };
        let close_command = spot_futures_order_draft_to_execution_command(
            &test_context(),
            "sf-close-1",
            &close,
            Utc::now(),
        );
        assert_eq!(close_command.side, OrderSide::Buy);
        assert_eq!(close_command.order_type, OrderType::ImmediateOrCancel);
        assert!(close_command.reduce_only);
        assert_eq!(
            close_command.metadata["role"],
            json!("perp_reduce_only_close_buy")
        );
    }
}
