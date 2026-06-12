use std::collections::{BTreeMap, HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_event_ledger::{EventIdentity, FundingSettlementLedgerRecord, LedgerEvent};
use rustcta_strategy_sdk::{
    AccountEvent, AccountPermission, ExecutionCancelCommand, ExecutionEvent, ExecutionOrderCommand,
    HealthSeverity, MarketDataChannel, MarketDataEvent, MarketDataSubscription, MarketType,
    OrderSide as SdkOrderSide, OrderType, RequiredAccountPermission, RiskCapability,
    RiskCapabilityDeclaration, StrategyCommandSchema, StrategyConfigSchema, StrategyContext,
    StrategyEvent, StrategyHealthIssue, StrategyInstanceId, StrategyRuntime, StrategySnapshot,
    StrategySnapshotSchema, StrategySpec, StrategyStatus, TimeInForce as SdkTimeInForce,
};
use rustcta_types::{
    AccountId as LedgerAccountId, CanonicalSymbol as LedgerCanonicalSymbol,
    ExchangeId as LedgerExchangeId, MarketType as LedgerMarketType,
    PositionSide as LedgerPositionSide, RunId as LedgerRunId, StrategyId as LedgerStrategyId,
    TenantId as LedgerTenantId,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod app_runtime;
pub mod core;
pub mod runtime_contract;

pub use app_runtime::{
    FundingSpreadAppRuntime, FundingSpreadExecutionIntentSummary, FundingSpreadNotification,
    FundingSpreadRuntimeCycle, FundingSpreadRuntimeInput, FundingSpreadStorageEvent,
};

pub use core::{
    evaluate_add, evaluate_close, evaluate_open, funding_rate_per_hour, leg_funding,
    net_funding_rate, net_funding_rate_for_basis, net_funding_rate_per_hour, next_order_notional,
    spread_pct, AddMode, AddingConfig, CanonicalSymbol, CloseEvaluation, CloseExecutionStyle,
    CloseReason, DirectionPolicy, EdgeBreakdown, EvaluationDecision, EvaluationRejectReason,
    ExchangeId, FeeRates, FundingConfig, FundingSnapshot, FundingSpreadExpansionMakerConfig,
    OpenEvaluation, OpenExecutionStyle, OrderBookTop, OrderDraft, OrderIntentKind, OrderSide,
    PositionSide, RiskConfig, RiskSnapshot, RouteConfig, RoutePosition, RouteState, SizingConfig,
    SpreadTargetDirection, SymbolPrecision, ThresholdsConfig, TimeInForce,
};
pub use runtime_contract::{
    build_runtime_contract, default_runtime_contract, FundingSpreadDashboardSnapshot,
    FundingSpreadDashboardSnapshotProvider, FundingSpreadExecutionProvider,
    FundingSpreadMarketDataProvider, FundingSpreadNotificationProvider,
    FundingSpreadRouteDashboardRow, FundingSpreadRuntimeContract, FundingSpreadRuntimeMode,
    FundingSpreadStorageProvider, RuntimeProviderContract, RuntimeTaskContract,
};

pub const STRATEGY_KIND: &str = "funding_spread_expansion_maker";
pub const DISPLAY_NAME: &str = "Funding Spread Expansion Maker";
pub const MIGRATED_FROM: &str = "new-strategy:funding_spread_expansion_maker";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingSpreadExpansionMakerStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for FundingSpreadExpansionMakerStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSpreadExpansionMakerSnapshotPayload {
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
    pub configured_exchanges: Vec<String>,
    pub configured_symbols: Vec<String>,
    pub route_count: usize,
    pub preferred_open_style: Option<OpenExecutionStyle>,
    pub preferred_close_style: Option<CloseExecutionStyle>,
    pub market_data_subscriptions: Vec<MarketDataSubscription>,
    pub route_runtime: Vec<FundingSpreadRouteRuntimeSnapshot>,
    pub submitted_order_count: u64,
    pub canceled_order_count: u64,
    pub pending_order_count: usize,
    pub last_submit_error: Option<String>,
    pub last_cancel_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSpreadRouteRuntimeSnapshot {
    pub route_id: String,
    pub state: RouteState,
    pub current_spread_pct: Option<f64>,
    pub net_funding_rate: Option<f64>,
    pub current_notional_usdt: f64,
    pub pending_maker_orders: usize,
    pub pending_hedge_orders: usize,
    pub last_open_reject_reason: Option<EvaluationRejectReason>,
    pub last_close_reason: Option<CloseReason>,
    pub last_submitted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct RuntimeMarketState {
    order_books: HashMap<String, OrderBookTop>,
    funding: HashMap<String, FundingSnapshot>,
    precision: HashMap<String, SymbolPrecision>,
    fees: HashMap<String, FeeRates>,
    risk: HashMap<String, RiskSnapshot>,
    positions: HashMap<String, RoutePosition>,
    last_open_evaluations: HashMap<String, OpenEvaluation>,
    last_close_evaluations: HashMap<String, CloseEvaluation>,
    submitted_route_actions: HashSet<String>,
    last_submitted_at: HashMap<String, DateTime<Utc>>,
    pending_orders: HashMap<String, PendingRuntimeOrder>,
    staged_hedges: HashMap<String, Vec<StagedHedgeOrder>>,
    applied_funding_settlements: HashSet<String>,
    funding_settlement_ledger_events: Vec<LedgerEvent>,
}

#[derive(Debug, Clone)]
struct PendingRuntimeOrder {
    route_id: String,
    action: String,
    client_order_id: String,
    exchange_id: String,
    symbol: String,
    submitted_at: DateTime<Utc>,
    post_only: bool,
    reduce_only: bool,
}

#[derive(Debug, Clone)]
struct StagedHedgeOrder {
    route_id: String,
    action: String,
    draft: OrderDraft,
}

#[derive(Debug, Clone, PartialEq)]
struct FundingSettlementUpdate {
    settlement_id: String,
    route_id: Option<String>,
    exchange: Option<ExchangeId>,
    canonical_symbol: Option<CanonicalSymbol>,
    position_side: Option<PositionSide>,
    notional_usdt: Option<f64>,
    funding_rate: Option<f64>,
    funding_pnl_usdt: f64,
    mark_price: Option<f64>,
    index_price: Option<f64>,
    settled_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct FundingSpreadExpansionMakerRuntime {
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
    config: Option<FundingSpreadExpansionMakerConfig>,
    ctx: Option<StrategyContext>,
    market_data_subscriptions: Vec<MarketDataSubscription>,
    state: RuntimeMarketState,
    submitted_order_count: u64,
    canceled_order_count: u64,
    last_submit_error: Option<String>,
    last_cancel_error: Option<String>,
}

impl FundingSpreadExpansionMakerRuntime {
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
            ctx: None,
            market_data_subscriptions: Vec::new(),
            state: RuntimeMarketState::default(),
            submitted_order_count: 0,
            canceled_order_count: 0,
            last_submit_error: None,
            last_cancel_error: None,
        }
    }

    fn snapshot_payload(&self) -> FundingSpreadExpansionMakerSnapshotPayload {
        let configured_exchanges = self
            .config
            .as_ref()
            .map(FundingSpreadExpansionMakerConfig::active_exchanges)
            .unwrap_or_default();
        let configured_symbols = self
            .config
            .as_ref()
            .map(FundingSpreadExpansionMakerConfig::active_symbols)
            .unwrap_or_default();
        let route_runtime = self.route_runtime_snapshots();
        FundingSpreadExpansionMakerSnapshotPayload {
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
            configured_exchanges,
            configured_symbols,
            route_count: self
                .config
                .as_ref()
                .map(|config| config.routes.len())
                .unwrap_or(0),
            preferred_open_style: self
                .config
                .as_ref()
                .map(|config| config.execution.preferred_open_style),
            preferred_close_style: self
                .config
                .as_ref()
                .map(|config| config.execution.preferred_close_style),
            market_data_subscriptions: self.market_data_subscriptions.clone(),
            route_runtime,
            submitted_order_count: self.submitted_order_count,
            canceled_order_count: self.canceled_order_count,
            pending_order_count: self.state.pending_orders.len()
                + self
                    .state
                    .staged_hedges
                    .values()
                    .map(Vec::len)
                    .sum::<usize>(),
            last_submit_error: self.last_submit_error.clone(),
            last_cancel_error: self.last_cancel_error.clone(),
        }
    }

    fn route_runtime_snapshots(&self) -> Vec<FundingSpreadRouteRuntimeSnapshot> {
        self.config
            .as_ref()
            .map(|config| {
                config
                    .routes
                    .iter()
                    .map(|route| {
                        let route_id = route.route_id.clone();
                        let position = self.state.positions.get(&route_id);
                        let spread = self.current_spread_pct(route);
                        FundingSpreadRouteRuntimeSnapshot {
                            route_id: route_id.clone(),
                            state: position
                                .map(|position| position.state)
                                .unwrap_or(RouteState::Observing),
                            current_spread_pct: spread,
                            net_funding_rate: self.current_net_funding_rate(route),
                            current_notional_usdt: position
                                .map(|position| position.current_notional_usdt)
                                .unwrap_or_default(),
                            pending_maker_orders: position
                                .map(|position| position.pending_maker_orders)
                                .unwrap_or_default(),
                            pending_hedge_orders: position
                                .map(|position| position.pending_hedge_orders)
                                .unwrap_or_default(),
                            last_open_reject_reason: self
                                .state
                                .last_open_evaluations
                                .get(&route_id)
                                .and_then(|evaluation| evaluation.reject_reason),
                            last_close_reason: self
                                .state
                                .last_close_evaluations
                                .get(&route_id)
                                .and_then(|evaluation| evaluation.reason),
                            last_submitted_at: self.state.last_submitted_at.get(&route_id).copied(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn ingest_market_data_event(&mut self, event: &MarketDataEvent) {
        let event_kind = event_kind(&event.payload);
        match event_kind.as_deref() {
            Some("order_book_top" | "orderbook_top" | "book_top") => {
                if let Some(book) = parse_order_book_top(event) {
                    self.state
                        .order_books
                        .insert(market_key(&book.exchange, &book.canonical_symbol), book);
                }
            }
            Some("funding_rate" | "funding") => {
                if let Some(funding) = parse_funding_snapshot(event) {
                    self.state.funding.insert(
                        market_key(&funding.exchange, &funding.canonical_symbol),
                        funding,
                    );
                }
            }
            _ => {}
        }
    }

    fn ingest_account_event(&mut self, event: &AccountEvent) {
        let event_kind = event_kind(&event.payload);
        match event_kind.as_deref() {
            Some("symbol_precision" | "precision") => {
                if let Some((exchange, symbol, precision)) = parse_symbol_precision(&event.payload)
                {
                    self.state
                        .precision
                        .insert(market_key(&exchange, &symbol), precision);
                }
            }
            Some("fee_rates" | "fees") => {
                if let Some((exchange, symbol, fees)) = parse_fee_rates(&event.payload) {
                    self.state.fees.insert(market_key(&exchange, &symbol), fees);
                }
            }
            Some("risk_snapshot" | "risk") => {
                if let Some((route_id, exchange, symbol, risk)) =
                    parse_route_risk_snapshot(&event.payload)
                {
                    if let Some(route_id) = route_id {
                        self.state.risk.insert(route_id, risk);
                    } else {
                        self.insert_matching_route_risk(exchange.as_ref(), symbol.as_ref(), risk);
                    }
                }
            }
            Some("route_position" | "position") => {
                if let Some(position) = parse_route_position(event) {
                    let route_id = position.route_id.clone();
                    self.state.positions.insert(route_id.clone(), position);
                    self.clear_submitted_route_actions(&route_id);
                }
            }
            Some("funding_settlement" | "funding_settlement_event" | "settlement") => {
                if let Some(settlement) = parse_funding_settlement(event) {
                    self.apply_funding_settlement(settlement);
                }
            }
            Some("route_closed" | "position_closed") => {
                if let Some(route_id) = envelope_string_field(&event.payload, &["route_id"]) {
                    self.state.positions.remove(&route_id);
                    self.clear_submitted_route_actions(&route_id);
                }
            }
            _ => {}
        }
    }

    fn ingest_execution_event(&mut self, event: &ExecutionEvent) {
        if let Some(client_order_id) = event
            .client_order_id
            .clone()
            .or_else(|| envelope_string_field(&event.payload, &["client_order_id"]))
        {
            if bool_field(&event.payload, &["terminal", "is_terminal"]).unwrap_or(false) {
                self.state.pending_orders.remove(&client_order_id);
            }
            if execution_fill_observed(&event.payload) {
                if let Some(staged) = self.state.staged_hedges.get_mut(&client_order_id) {
                    if let Some(fill_quantity) = execution_fill_quantity(&event.payload) {
                        for hedge in staged {
                            hedge.draft.base_quantity =
                                hedge.draft.base_quantity.min(fill_quantity);
                            hedge.draft.order_quantity =
                                hedge.draft.order_quantity.min(fill_quantity);
                        }
                    }
                }
            } else if bool_field(&event.payload, &["canceled", "cancelled"]).unwrap_or(false) {
                self.state.staged_hedges.remove(&client_order_id);
            }
        }
        let route_id = envelope_string_field(&event.payload, &["route_id"]);
        if bool_field(&event.payload, &["terminal", "is_terminal"]).unwrap_or(false) {
            if let Some(route_id) = route_id.as_ref() {
                self.clear_submitted_route_actions(route_id);
            }
        }
        if bool_field(&event.payload, &["unknown_order_state"]).unwrap_or(false) {
            if let Some(route_id) = route_id.as_ref() {
                if let Some(position) = self.state.positions.get_mut(route_id) {
                    position.unknown_order_state = true;
                }
            }
        }
    }

    fn apply_funding_settlement(&mut self, settlement: FundingSettlementUpdate) {
        if !self
            .state
            .applied_funding_settlements
            .insert(settlement.settlement_id.clone())
        {
            return;
        }
        let mut route_position = None;
        if let Some(route_id) = settlement.route_id.as_ref() {
            if let Some(position) = self.state.positions.get_mut(route_id) {
                position.cumulative_funding_pnl_usdt += settlement.funding_pnl_usdt;
                route_position = Some(position.clone());
            }
        } else {
            for position in self.state.positions.values_mut() {
                let symbol_matches = settlement
                    .canonical_symbol
                    .as_ref()
                    .is_none_or(|symbol| symbol == &position.canonical_symbol);
                let exchange_matches = settlement.exchange.as_ref().is_none_or(|exchange| {
                    exchange == &position.long_exchange || exchange == &position.short_exchange
                });
                if symbol_matches && exchange_matches {
                    position.cumulative_funding_pnl_usdt += settlement.funding_pnl_usdt;
                    route_position = Some(position.clone());
                    break;
                }
            }
        }
        if let Some(event) =
            self.funding_settlement_ledger_event(&settlement, route_position.as_ref())
        {
            self.state.funding_settlement_ledger_events.push(event);
        }
    }

    fn funding_settlement_ledger_event(
        &self,
        settlement: &FundingSettlementUpdate,
        route_position: Option<&RoutePosition>,
    ) -> Option<LedgerEvent> {
        let exchange = settlement.exchange.as_ref().or_else(|| {
            let route_position = route_position?;
            match settlement.position_side {
                Some(PositionSide::Long) => Some(&route_position.long_exchange),
                Some(PositionSide::Short) => Some(&route_position.short_exchange),
                None => None,
            }
        })?;
        let symbol = settlement
            .canonical_symbol
            .as_ref()
            .or_else(|| route_position.map(|position| &position.canonical_symbol))?;
        let position_side = settlement.position_side.or_else(|| {
            route_position.and_then(|position| position_side_for_exchange(exchange, position))
        })?;
        let notional_usdt = settlement
            .notional_usdt
            .filter(|notional| notional.is_finite() && *notional >= 0.0)
            .or_else(|| route_position.map(|position| position.current_notional_usdt.max(0.0)))
            .unwrap_or(0.0);
        let funding_rate = settlement
            .funding_rate
            .filter(|rate| rate.is_finite())
            .unwrap_or_else(|| {
                if notional_usdt > 0.0 {
                    settlement.funding_pnl_usdt / notional_usdt
                } else {
                    0.0
                }
            });
        let identity = self.funding_settlement_identity(settlement)?;
        let mut record = FundingSettlementLedgerRecord::new(
            identity,
            settlement.settlement_id.clone(),
            ledger_exchange_id(exchange)?,
            LedgerMarketType::Perpetual,
            ledger_canonical_symbol(symbol)?,
            ledger_position_side(position_side),
            notional_usdt,
            funding_rate,
            settlement.funding_pnl_usdt,
            settlement.settled_at,
        );
        record.mark_price = settlement
            .mark_price
            .filter(|price| price.is_finite() && *price > 0.0);
        record.index_price = settlement
            .index_price
            .filter(|price| price.is_finite() && *price > 0.0);
        record.metadata = json!({
            "source": STRATEGY_KIND,
            "route_id": settlement.route_id.as_deref(),
            "exchange": exchange.to_string(),
            "canonical_symbol": symbol.as_pair(),
        });
        record.validated().ok()?;
        Some(LedgerEvent::funding_settlement(record))
    }

    fn funding_settlement_identity(
        &self,
        settlement: &FundingSettlementUpdate,
    ) -> Option<EventIdentity> {
        let tenant_id = LedgerTenantId::new(
            self.ctx
                .as_ref()
                .map(|ctx| ctx.tenant_id().to_string())
                .unwrap_or_else(|| "default".to_string()),
        )
        .ok()?;
        let mut identity = EventIdentity::new(tenant_id, STRATEGY_KIND, settlement.settled_at)
            .with_correlation_id(settlement.settlement_id.clone())
            .with_idempotency_key(settlement.settlement_id.clone());
        if let Some(ctx) = self.ctx.as_ref() {
            if let Ok(account_id) = LedgerAccountId::new(ctx.account_id()) {
                identity = identity.with_account(account_id);
            }
            if let (Ok(strategy_id), Ok(run_id)) = (
                LedgerStrategyId::new(ctx.strategy_id()),
                LedgerRunId::new(ctx.run_id()),
            ) {
                identity = identity.with_strategy_run(strategy_id, run_id);
            }
        }
        Some(identity)
    }

    async fn evaluate_routes_and_submit(&mut self, now: DateTime<Utc>) -> anyhow::Result<()> {
        if !matches!(self.status, StrategyStatus::Running) {
            return Ok(());
        }
        let Some(config) = self.config.clone() else {
            return Ok(());
        };
        for route in config.routes.iter().filter(|route| route.enabled) {
            self.evaluate_route(&config, route, now).await?;
        }
        Ok(())
    }

    async fn evaluate_route(
        &mut self,
        config: &FundingSpreadExpansionMakerConfig,
        route: &RouteConfig,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        if self
            .cancel_stale_or_invalid_pending_orders(config, route, now)
            .await?
        {
            return Ok(());
        }
        if self.route_has_submitted_action(&route.route_id) {
            return Ok(());
        }

        let Some((leg_a_book, leg_b_book)) = self.route_books(route) else {
            return Ok(());
        };
        let route_id = route.route_id.clone();
        let leg_a_key = market_key(&route.leg_a(), &route.canonical_symbol());
        let leg_b_key = market_key(&route.leg_b(), &route.canonical_symbol());
        let leg_a_precision = self.state.precision.get(&leg_a_key).copied();
        let leg_b_precision = self.state.precision.get(&leg_b_key).copied();
        let leg_a_fees = self.state.fees.get(&leg_a_key).copied().unwrap_or_default();
        let leg_b_fees = self.state.fees.get(&leg_b_key).copied().unwrap_or_default();
        let leg_a_funding = self.state.funding.get(&leg_a_key);
        let leg_b_funding = self.state.funding.get(&leg_b_key);
        let mut risk_snapshot = self.state.risk.get(&route_id).copied().unwrap_or_default();
        risk_snapshot.precision_ready =
            risk_snapshot.precision_ready && leg_a_precision.is_some() && leg_b_precision.is_some();
        if let Some(position) = self.state.positions.get(&route_id) {
            risk_snapshot.pending_repair = risk_snapshot.pending_repair
                || position.pending_repair
                || matches!(position.state, RouteState::RepairingSingleLeg);
            risk_snapshot.unknown_order_state =
                risk_snapshot.unknown_order_state || position.unknown_order_state;
            if position.leg_imbalance_usdt() > config.sizing.max_leg_imbalance_usdt {
                risk_snapshot.single_leg_exposure_ms = risk_snapshot
                    .single_leg_exposure_ms
                    .max(config.risk.single_leg_timeout_ms.saturating_add(1));
            }
        }
        let leg_a_precision = leg_a_precision.unwrap_or_default();
        let leg_b_precision = leg_b_precision.unwrap_or_default();

        let current_position = self.state.positions.get(&route_id).cloned();
        if let Some(position) = current_position.as_ref() {
            let close_only_state = matches!(
                position.state,
                RouteState::RepairingSingleLeg | RouteState::CloseOnly | RouteState::RiskStopped
            );
            if !close_only_state
                && !matches!(
                    position.state,
                    RouteState::Open | RouteState::AddPending | RouteState::TargetCloseReady
                )
            {
                return Ok(());
            }
            let net_funding = self.current_net_funding_rate(route).unwrap_or_default();
            let close_eval = evaluate_close(
                route,
                position,
                &leg_a_book,
                &leg_b_book,
                leg_a_precision,
                leg_b_precision,
                risk_snapshot,
                net_funding,
                config,
                now,
            );
            self.state
                .last_close_evaluations
                .insert(route_id.clone(), close_eval.clone());
            if close_eval.should_close {
                self.submit_route_orders(config, route, "close", close_eval.orders.as_slice(), now)
                    .await?;
                return Ok(());
            }
            if close_only_state {
                return Ok(());
            }

            let add_eval = evaluate_add(
                route,
                position,
                &leg_a_book,
                &leg_b_book,
                leg_a_precision,
                leg_b_precision,
                leg_a_fees,
                leg_b_fees,
                leg_a_funding,
                leg_b_funding,
                risk_snapshot,
                config,
                now,
            );
            self.state
                .last_open_evaluations
                .insert(route_id.clone(), add_eval.clone());
            if add_eval.decision == EvaluationDecision::Accepted {
                self.submit_route_orders(config, route, "add", add_eval.orders.as_slice(), now)
                    .await?;
            }
            return Ok(());
        }

        let open_eval = evaluate_open(
            route,
            &leg_a_book,
            &leg_b_book,
            leg_a_precision,
            leg_b_precision,
            leg_a_fees,
            leg_b_fees,
            leg_a_funding,
            leg_b_funding,
            0.0,
            risk_snapshot,
            config,
            now,
        );
        self.state
            .last_open_evaluations
            .insert(route_id.clone(), open_eval.clone());
        if open_eval.decision == EvaluationDecision::Accepted {
            self.submit_route_orders(config, route, "open", open_eval.orders.as_slice(), now)
                .await?;
        }
        Ok(())
    }

    async fn submit_route_orders(
        &mut self,
        config: &FundingSpreadExpansionMakerConfig,
        route: &RouteConfig,
        action: &str,
        orders: &[OrderDraft],
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        if orders.is_empty() || !self.should_submit_orders(config) {
            return Ok(());
        }
        let Some(ctx) = self.ctx.clone() else {
            return Ok(());
        };
        let action_key = format!("{}:{action}", route.route_id);
        if !self.state.submitted_route_actions.insert(action_key) {
            return Ok(());
        }

        let stage_non_post_only_hedges = orders.iter().any(|order| order.post_only)
            && orders.iter().any(|order| !order.post_only);
        let mut first_post_only_client_order_id: Option<String> = None;
        for (index, draft) in orders.iter().enumerate() {
            if stage_non_post_only_hedges && !draft.post_only {
                if let Some(client_order_id) = first_post_only_client_order_id.as_ref() {
                    self.state
                        .staged_hedges
                        .entry(client_order_id.clone())
                        .or_default()
                        .push(StagedHedgeOrder {
                            route_id: route.route_id.clone(),
                            action: action.to_string(),
                            draft: draft.clone(),
                        });
                }
                continue;
            }
            if let Some(client_order_id) = self
                .submit_single_order(&ctx, route, action, draft, index, now)
                .await?
            {
                if draft.post_only && first_post_only_client_order_id.is_none() {
                    first_post_only_client_order_id = Some(client_order_id);
                }
            } else {
                break;
            }
        }
        self.state
            .last_submitted_at
            .insert(route.route_id.clone(), now);
        self.mark_route_pending(route, action, orders, now);
        Ok(())
    }

    async fn submit_single_order(
        &mut self,
        ctx: &StrategyContext,
        route: &RouteConfig,
        action: &str,
        draft: &OrderDraft,
        index: usize,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Option<String>> {
        let sequence = self.submitted_order_count + 1;
        let client_order_id = format!(
            "{}-{}-{action}-{sequence}-{index}",
            STRATEGY_KIND, route.route_id
        );
        let mut command = order_draft_to_command(ctx, draft, &route.route_id, client_order_id, now);
        command
            .metadata
            .insert("route_id".to_string(), json!(route.route_id));
        command
            .metadata
            .insert("route_action".to_string(), json!(action));
        let ack = ctx.execution().submit_order(command).await;
        match ack {
            Ok(ack) if ack.accepted => {
                self.submitted_order_count += 1;
                self.last_submit_error = None;
                let client_order_id = ack.client_order_id;
                self.state.pending_orders.insert(
                    client_order_id.clone(),
                    PendingRuntimeOrder {
                        route_id: route.route_id.clone(),
                        action: action.to_string(),
                        client_order_id: client_order_id.clone(),
                        exchange_id: draft.exchange.to_string(),
                        symbol: draft.canonical_symbol.as_pair(),
                        submitted_at: now,
                        post_only: draft.post_only,
                        reduce_only: draft.reduce_only,
                    },
                );
                Ok(Some(client_order_id))
            }
            Ok(ack) => {
                self.last_submit_error = ack.reason.or(Some("order rejected".to_string()));
                Ok(None)
            }
            Err(error) => {
                self.last_submit_error = Some(error.to_string());
                Ok(None)
            }
        }
    }

    async fn submit_staged_hedges_for_event(
        &mut self,
        event: &ExecutionEvent,
    ) -> anyhow::Result<()> {
        if !execution_fill_observed(&event.payload) {
            return Ok(());
        }
        let Some(client_order_id) = event
            .client_order_id
            .clone()
            .or_else(|| envelope_string_field(&event.payload, &["client_order_id"]))
        else {
            return Ok(());
        };
        let Some(staged) = self.state.staged_hedges.remove(&client_order_id) else {
            return Ok(());
        };
        let Some(config) = self.config.clone() else {
            return Ok(());
        };
        if !self.should_submit_orders(&config) {
            return Ok(());
        }
        let Some(ctx) = self.ctx.clone() else {
            return Ok(());
        };
        for (index, hedge) in staged.iter().enumerate() {
            let Some(route) = config
                .routes
                .iter()
                .find(|route| route.route_id == hedge.route_id)
            else {
                continue;
            };
            self.submit_single_order(
                &ctx,
                route,
                &format!("{}_hedge", hedge.action),
                &hedge.draft,
                index,
                event.occurred_at,
            )
            .await?;
        }
        if let Some(route_id) = envelope_string_field(&event.payload, &["route_id"]) {
            self.refresh_pending_route_position_by_id(&route_id);
        }
        Ok(())
    }

    async fn cancel_stale_or_invalid_pending_orders(
        &mut self,
        config: &FundingSpreadExpansionMakerConfig,
        route: &RouteConfig,
        now: DateTime<Utc>,
    ) -> anyhow::Result<bool> {
        if !self.should_submit_orders(config) {
            return Ok(false);
        }
        let pending: Vec<PendingRuntimeOrder> = self
            .state
            .pending_orders
            .values()
            .filter(|order| order.route_id == route.route_id && order.post_only)
            .cloned()
            .collect();
        if pending.is_empty() {
            return Ok(false);
        }

        let signal_invalid = self.route_signal_invalid_for_pending(config, route);
        let stale_client_order_ids: Vec<String> = pending
            .iter()
            .filter(|order| {
                let timeout_ms = if order.action == "close" {
                    config.execution.close_maker_order_timeout_ms
                } else {
                    config.execution.maker_order_timeout_ms
                };
                now.signed_duration_since(order.submitted_at)
                    .num_milliseconds()
                    >= i64::try_from(timeout_ms).unwrap_or(i64::MAX)
                    || signal_invalid
            })
            .map(|order| order.client_order_id.clone())
            .collect();
        let canceled_any = !stale_client_order_ids.is_empty();
        for client_order_id in stale_client_order_ids {
            self.cancel_pending_order(route, &client_order_id, now)
                .await?;
        }
        self.refresh_pending_route_position(route);
        Ok(canceled_any)
    }

    async fn cancel_pending_order(
        &mut self,
        route: &RouteConfig,
        client_order_id: &str,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let Some(ctx) = self.ctx.clone() else {
            return Ok(());
        };
        let Some(order) = self.state.pending_orders.get(client_order_id).cloned() else {
            return Ok(());
        };
        let command = ExecutionCancelCommand {
            schema_version: 1,
            tenant_id: ctx.tenant_id().to_string(),
            account_id: ctx.account_id().to_string(),
            strategy_id: ctx.strategy_id().to_string(),
            run_id: ctx.run_id().to_string(),
            client_order_id: Some(order.client_order_id.clone()),
            execution_order_id: None,
            idempotency_key: execution_idempotency_key(
                &ctx,
                &format!("cancel-{}", order.client_order_id),
            ),
            risk_profile_id: route.route_id.clone(),
            requested_at: now,
            exchange_id: order.exchange_id.clone(),
            symbol: order.symbol.clone(),
            metadata: BTreeMap::from([
                ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
                ("route_id".to_string(), json!(route.route_id)),
                ("route_action".to_string(), json!(order.action)),
                ("post_only".to_string(), json!(order.post_only)),
                ("reduce_only".to_string(), json!(order.reduce_only)),
                (
                    "cancel_reason".to_string(),
                    json!("maker_timeout_or_signal_invalid"),
                ),
            ]),
        };
        match ctx.execution().cancel_order(command).await {
            Ok(ack) if ack.accepted => {
                self.canceled_order_count += 1;
                self.last_cancel_error = None;
                self.state.pending_orders.remove(client_order_id);
                self.state.staged_hedges.remove(client_order_id);
            }
            Ok(ack) => {
                self.last_cancel_error = ack.reason.or(Some("cancel rejected".to_string()));
            }
            Err(error) => {
                self.last_cancel_error = Some(error.to_string());
            }
        }
        Ok(())
    }

    fn route_signal_invalid_for_pending(
        &self,
        config: &FundingSpreadExpansionMakerConfig,
        route: &RouteConfig,
    ) -> bool {
        let Some(position) = self.state.positions.get(&route.route_id) else {
            return false;
        };
        let Some(spread) = self.current_spread_pct(route) else {
            return false;
        };
        match position.state {
            RouteState::MakerOpenPending | RouteState::AddPending => {
                config.execution.cancel_if_signal_invalid
                    && (!route
                        .target_direction
                        .open_threshold_met(spread, config.thresholds.open_spread_pct)
                        || self
                            .current_net_funding_rate(route)
                            .is_some_and(|rate| rate < config.thresholds.min_net_funding_rate))
            }
            RouteState::Closing | RouteState::TargetCloseReady => {
                if !config.execution.cancel_if_close_signal_invalid {
                    return false;
                }
                let risk_snapshot = self
                    .state
                    .risk
                    .get(&route.route_id)
                    .copied()
                    .unwrap_or_default();
                let net_funding = self.current_net_funding_rate(route).unwrap_or_default();
                let risk_close = risk_snapshot
                    .risk_close_reason(&config.thresholds, net_funding)
                    .is_some();
                !risk_close
                    && !route
                        .target_direction
                        .target_close_met(spread, config.thresholds.target_close_spread_pct)
            }
            _ => false,
        }
    }

    fn refresh_pending_route_position(&mut self, route: &RouteConfig) {
        self.refresh_pending_route_position_by_id(&route.route_id);
    }

    fn refresh_pending_route_position_by_id(&mut self, route_id: &str) {
        let route_id = route_id.to_string();
        let pending_maker_count = self
            .state
            .pending_orders
            .values()
            .filter(|order| order.route_id == route_id)
            .filter(|order| order.post_only)
            .count();
        let pending_hedge_count = self
            .state
            .pending_orders
            .values()
            .filter(|order| order.route_id == route_id)
            .filter(|order| !order.post_only)
            .count();
        let staged_hedge_count = self
            .state
            .staged_hedges
            .values()
            .flat_map(|hedges| hedges.iter())
            .filter(|hedge| hedge.route_id == route_id)
            .count();
        let no_pending_for_route =
            pending_maker_count == 0 && pending_hedge_count == 0 && staged_hedge_count == 0;
        if let Some(position) = self.state.positions.get_mut(&route_id) {
            position.pending_maker_orders = pending_maker_count;
            position.pending_hedge_orders = pending_hedge_count + staged_hedge_count;
            if no_pending_for_route {
                if position.current_notional_usdt <= 0.0 {
                    self.state.positions.remove(&route_id);
                } else if matches!(position.state, RouteState::Closing | RouteState::AddPending) {
                    position.state = RouteState::Open;
                }
            }
        }
        if no_pending_for_route {
            self.clear_submitted_route_actions(&route_id);
        }
    }

    fn mark_route_pending(
        &mut self,
        route: &RouteConfig,
        action: &str,
        orders: &[OrderDraft],
        now: DateTime<Utc>,
    ) {
        let pending_maker_orders = orders.iter().filter(|order| order.post_only).count();
        let pending_hedge_orders = orders.iter().filter(|order| !order.post_only).count();
        match action {
            "open" | "add" => {
                let symbol = route.canonical_symbol();
                let (long_exchange, short_exchange) = core::route_sides(route);
                let current_spread_pct = self.current_spread_pct(route).unwrap_or(0.0);
                let position = self
                    .state
                    .positions
                    .entry(route.route_id.clone())
                    .or_insert_with(|| RoutePosition {
                        route_id: route.route_id.clone(),
                        state: RouteState::MakerOpenPending,
                        long_exchange: long_exchange.clone(),
                        short_exchange: short_exchange.clone(),
                        canonical_symbol: symbol.clone(),
                        long_base_quantity: 0.0,
                        short_base_quantity: 0.0,
                        long_avg_entry_price: 0.0,
                        short_avg_entry_price: 0.0,
                        weighted_open_spread_pct: current_spread_pct,
                        current_notional_usdt: 0.0,
                        cumulative_funding_pnl_usdt: 0.0,
                        cumulative_fee_usdt: 0.0,
                        opened_at: now,
                        last_add_at: None,
                        last_add_spread_pct: None,
                        open_slices: 0,
                        pending_maker_orders: 0,
                        pending_hedge_orders: 0,
                        unknown_order_state: false,
                        pending_repair: false,
                    });
                position.state = if action == "add" {
                    RouteState::AddPending
                } else {
                    RouteState::MakerOpenPending
                };
                position.pending_maker_orders = pending_maker_orders;
                position.pending_hedge_orders = pending_hedge_orders;
            }
            "close" => {
                if let Some(position) = self.state.positions.get_mut(&route.route_id) {
                    position.state = RouteState::Closing;
                    position.pending_maker_orders = pending_maker_orders;
                    position.pending_hedge_orders = pending_hedge_orders;
                }
            }
            _ => {}
        }
    }

    fn route_books(&self, route: &RouteConfig) -> Option<(OrderBookTop, OrderBookTop)> {
        let symbol = route.canonical_symbol();
        let leg_a = self
            .state
            .order_books
            .get(&market_key(&route.leg_a(), &symbol))?
            .clone();
        let leg_b = self
            .state
            .order_books
            .get(&market_key(&route.leg_b(), &symbol))?
            .clone();
        Some((leg_a, leg_b))
    }

    fn current_spread_pct(&self, route: &RouteConfig) -> Option<f64> {
        let (leg_a, leg_b) = self.route_books(route)?;
        Some(spread_pct(mid_price(&leg_a), mid_price(&leg_b)))
    }

    fn current_net_funding_rate(&self, route: &RouteConfig) -> Option<f64> {
        let symbol = route.canonical_symbol();
        let leg_a_funding = self
            .state
            .funding
            .get(&market_key(&route.leg_a(), &symbol))?;
        let leg_b_funding = self
            .state
            .funding
            .get(&market_key(&route.leg_b(), &symbol))?;
        let (long_exchange, _) = core::route_sides(route);
        let (long_rate, long_interval, short_rate, short_interval) =
            if long_exchange == route.leg_a() {
                (
                    leg_a_funding.funding_rate,
                    leg_a_funding.funding_interval_hours,
                    leg_b_funding.funding_rate,
                    leg_b_funding.funding_interval_hours,
                )
            } else {
                (
                    leg_b_funding.funding_rate,
                    leg_b_funding.funding_interval_hours,
                    leg_a_funding.funding_rate,
                    leg_a_funding.funding_interval_hours,
                )
            };
        let basis_hours = self
            .config
            .as_ref()
            .map(|config| config.funding.funding_rate_basis_hours)
            .unwrap_or(8.0);
        Some(net_funding_rate_for_basis(
            long_rate,
            long_interval,
            short_rate,
            short_interval,
            basis_hours,
        ))
    }

    fn insert_matching_route_risk(
        &mut self,
        exchange: Option<&ExchangeId>,
        symbol: Option<&CanonicalSymbol>,
        risk: RiskSnapshot,
    ) {
        let Some(config) = self.config.as_ref() else {
            return;
        };
        for route in config.routes.iter().filter(|route| route.enabled) {
            let symbol_matches = symbol
                .map(|symbol| symbol == &route.canonical_symbol())
                .unwrap_or(true);
            let exchange_matches = exchange
                .map(|exchange| exchange == &route.leg_a() || exchange == &route.leg_b())
                .unwrap_or(true);
            if symbol_matches && exchange_matches {
                self.state.risk.insert(route.route_id.clone(), risk);
            }
        }
    }

    fn should_submit_orders(&self, config: &FundingSpreadExpansionMakerConfig) -> bool {
        config.mode.eq_ignore_ascii_case("live")
            && !config.dry_run
            && matches!(self.status, StrategyStatus::Running)
    }

    fn route_has_submitted_action(&self, route_id: &str) -> bool {
        let prefix = format!("{route_id}:");
        self.state
            .submitted_route_actions
            .iter()
            .any(|action| action.starts_with(&prefix))
    }

    fn clear_submitted_route_actions(&mut self, route_id: &str) {
        let prefix = format!("{route_id}:");
        self.state
            .submitted_route_actions
            .retain(|action| !action.starts_with(&prefix));
    }
}

impl Default for FundingSpreadExpansionMakerRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for FundingSpreadExpansionMakerRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        let config: FundingSpreadExpansionMakerConfig =
            serde_json::from_value(ctx.config().clone())?;
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.last_event_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        self.market_data_subscriptions = funding_spread_market_data_subscriptions(&config);
        self.config = Some(config);
        self.ctx = Some(ctx);
        self.handled_events = 0;
        self.market_data_events = 0;
        self.execution_events = 0;
        self.account_events = 0;
        self.operator_commands = 0;
        self.timer_events = 0;
        self.last_market_data_at = None;
        self.last_execution_at = None;
        self.last_account_sync_at = None;
        self.state = RuntimeMarketState::default();
        self.submitted_order_count = 0;
        self.canceled_order_count = 0;
        self.last_submit_error = None;
        self.last_cancel_error = None;
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
                self.ingest_execution_event(event);
                self.submit_staged_hedges_for_event(event).await?;
            }
            StrategyEvent::MarketData(event) => {
                self.market_data_events += 1;
                self.last_market_data_at = Some(event.received_at);
                self.ingest_market_data_event(event);
            }
            StrategyEvent::Account(event) => {
                self.account_events += 1;
                self.last_account_sync_at = Some(event.received_at);
                self.ingest_account_event(event);
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
        self.evaluate_routes_and_submit(event_timestamp(&event))
            .await?;
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
            "Cross-exchange perpetual funding plus spread-expansion maker strategy.".to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places maker/taker perpetual orders",
            ),
            risk_capability(RiskCapability::CancelOrders, "Cancels stale maker orders"),
            risk_capability(
                RiskCapability::ReduceOnlyOrders,
                "Closes route positions reduce-only",
            ),
            risk_capability(RiskCapability::Hedging, "Maintains hedged perp exposure"),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Reserves route notional before opening",
            ),
            risk_capability(
                RiskCapability::CrossAccountRead,
                "Reads balances, positions, orders, fills and funding state",
            ),
        ],
        market_data_subscriptions: funding_spread_market_data_subscriptions(
            &FundingSpreadExpansionMakerConfig::default(),
        ),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadPositions,
            AccountPermission::ReadOrders,
            AccountPermission::ReadFills,
            AccountPermission::TradePerpetual,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            ("legacy_module".to_string(), json!(MIGRATED_FROM)),
            ("runtime_contract_migration".to_string(), json!(true)),
            (
                "primary_market_type".to_string(),
                json!(MarketType::Perpetual),
            ),
            (
                "execution_styles".to_string(),
                json!([
                    "maker_taker",
                    "dual_maker",
                    "dual_taker",
                    "maker_maker_reduce_only",
                    "maker_taker_reduce_only",
                    "dual_taker_reduce_only"
                ]),
            ),
        ]),
    }
}

pub fn funding_spread_market_data_subscriptions(
    config: &FundingSpreadExpansionMakerConfig,
) -> Vec<MarketDataSubscription> {
    let channels = vec![
        MarketDataChannel::OrderBookTop,
        MarketDataChannel::OrderBookDepth,
        MarketDataChannel::FundingRate,
        MarketDataChannel::MarkPrice,
        MarketDataChannel::IndexPrice,
    ];
    config
        .routes
        .iter()
        .filter(|route| route.enabled)
        .flat_map(|route| {
            let symbol = route.canonical_symbol().as_pair();
            [
                (route.leg_a_exchange.clone(), symbol.clone()),
                (route.leg_b_exchange.clone(), symbol),
            ]
        })
        .map(|(exchange_id, symbol)| MarketDataSubscription {
            exchange_id: exchange_id.trim().to_ascii_lowercase(),
            symbol,
            market_type: MarketType::Perpetual,
            channels: channels.clone(),
        })
        .collect()
}

pub fn order_draft_to_command(
    ctx: &StrategyContext,
    draft: &OrderDraft,
    risk_profile_id: &str,
    client_order_id: String,
    requested_at: DateTime<Utc>,
) -> ExecutionOrderCommand {
    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: client_order_id.clone(),
        idempotency_key: execution_idempotency_key(ctx, &client_order_id),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: draft.exchange.to_string(),
        symbol: draft.canonical_symbol.as_pair(),
        side: sdk_side(draft.side),
        order_type: if draft.post_only {
            OrderType::Limit
        } else {
            OrderType::Limit
        },
        quantity: trim_float(draft.order_quantity),
        price: Some(trim_float(draft.limit_price)),
        time_in_force: Some(sdk_time_in_force(draft.time_in_force)),
        reduce_only: draft.reduce_only,
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            ("intent_kind".to_string(), json!(draft.intent_kind)),
            ("position_side".to_string(), json!(draft.position_side)),
            ("post_only".to_string(), json!(draft.post_only)),
            (
                "reference_price".to_string(),
                json!(trim_float(draft.reference_price)),
            ),
            (
                "base_quantity".to_string(),
                json!(trim_float(draft.base_quantity)),
            ),
            (
                "execution_style_contract".to_string(),
                json!("funding_spread_expansion_maker"),
            ),
        ]),
    }
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": true,
            "properties": {
                "strategy_kind": { "type": ["string", "null"] },
                "display_name": { "type": ["string", "null"] },
                "mode": { "type": "string", "default": "observe" },
                "dry_run": { "type": "boolean", "default": true },
                "routes": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["route_id", "leg_a_exchange", "leg_b_exchange", "symbol"],
                        "properties": {
                            "route_id": { "type": "string", "minLength": 1 },
                            "leg_a_exchange": { "type": "string", "minLength": 1 },
                            "leg_b_exchange": { "type": "string", "minLength": 1 },
                            "symbol": { "type": "string", "minLength": 1 },
                            "target_direction": { "enum": ["decrease", "increase"] },
                            "direction_policy": { "enum": ["funding_first", "spread_first", "balanced"] },
                            "enabled": { "type": "boolean" }
                        }
                    }
                },
                "thresholds": { "type": "object" },
                "funding": { "type": "object" },
                "sizing": { "type": "object" },
                "adding": { "type": "object" },
                "execution": { "type": "object" },
                "risk": { "type": "object" },
                "runtime_contract": { "type": ["object", "null"] }
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
            "required": ["migrated_from", "handled_events", "configured_exchanges", "configured_symbols"],
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
                "configured_exchanges": { "type": "array", "items": { "type": "string" } },
                "configured_symbols": { "type": "array", "items": { "type": "string" } },
                "route_count": { "type": "integer", "minimum": 0 },
                "preferred_open_style": { "type": ["string", "null"] },
                "preferred_close_style": { "type": ["string", "null"] },
                "market_data_subscriptions": { "type": "array" },
                "route_runtime": { "type": "array" },
                "submitted_order_count": { "type": "integer", "minimum": 0 },
                "canceled_order_count": { "type": "integer", "minimum": 0 },
                "pending_order_count": { "type": "integer", "minimum": 0 },
                "last_submit_error": { "type": ["string", "null"] },
                "last_cancel_error": { "type": ["string", "null"] }
            }
        }),
    }
}

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    [
        "pause",
        "resume",
        "stop",
        "refresh_routes",
        "enable_route",
        "disable_route",
        "close_route",
        "close_all",
        "sync_positions",
        "repair_route",
    ]
    .into_iter()
    .map(|command_kind| StrategyCommandSchema {
        command_kind: command_kind.to_string(),
        description: Some(format!(
            "Funding spread expansion maker runtime {command_kind} command"
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
        "market_data_stale",
        "No recent funding/spread market data event observed",
        900,
    );
    push_staleness_issue(
        &mut issues,
        now,
        started_at,
        last_account_sync_at,
        "account_sync_stale",
        "No recent account sync event observed",
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

fn event_kind(payload: &Value) -> Option<String> {
    envelope_string_field(payload, &["event_kind", "event_type", "kind", "type"])
}

fn parse_order_book_top(event: &MarketDataEvent) -> Option<OrderBookTop> {
    let body = event_payload_body(&event.payload);
    let exchange = ExchangeId::new(
        envelope_string_field(&event.payload, &["exchange", "exchange_id"])
            .unwrap_or_else(|| event.exchange_id.clone()),
    );
    let symbol = parse_payload_symbol(&event.payload)
        .or_else(|| parse_payload_symbol(body))
        .unwrap_or_else(|| parse_event_symbol(&event.symbol));
    Some(OrderBookTop {
        exchange,
        canonical_symbol: symbol,
        best_bid_price: f64_field(body, &["best_bid_price", "bid_price", "bid"])?,
        best_bid_quantity: f64_field(
            body,
            &[
                "best_bid_quantity",
                "bid_quantity",
                "bid_qty",
                "bid_quantity_base",
            ],
        )?,
        best_ask_price: f64_field(body, &["best_ask_price", "ask_price", "ask"])?,
        best_ask_quantity: f64_field(
            body,
            &[
                "best_ask_quantity",
                "ask_quantity",
                "ask_qty",
                "ask_quantity_base",
            ],
        )?,
        levels: usize_field(body, &["levels"]).unwrap_or(1),
        received_at: datetime_field(&event.payload, &["received_at"])
            .or_else(|| datetime_field(body, &["received_at"]))
            .unwrap_or(event.received_at),
    })
}

fn parse_funding_snapshot(event: &MarketDataEvent) -> Option<FundingSnapshot> {
    let body = event_payload_body(&event.payload);
    let exchange = ExchangeId::new(
        envelope_string_field(&event.payload, &["exchange", "exchange_id"])
            .unwrap_or_else(|| event.exchange_id.clone()),
    );
    let symbol = parse_payload_symbol(&event.payload)
        .or_else(|| parse_payload_symbol(body))
        .unwrap_or_else(|| parse_event_symbol(&event.symbol));
    Some(FundingSnapshot {
        exchange,
        canonical_symbol: symbol,
        funding_rate: f64_field(body, &["funding_rate", "rate"])?,
        predicted_funding_rate: f64_field(body, &["predicted_funding_rate", "predicted_rate"]),
        mark_price: f64_field(body, &["mark_price", "mark"]),
        index_price: f64_field(body, &["index_price", "index"]),
        open_interest: f64_field(body, &["open_interest", "oi"]),
        turnover_24h: f64_field(body, &["turnover_24h", "quote_volume_24h", "amount24h"]),
        volume_24h: f64_field(body, &["volume_24h", "base_volume_24h", "volume24h"]),
        funding_interval_hours: f64_field(
            body,
            &[
                "funding_interval_hours",
                "interval_hours",
                "settlement_interval_hours",
            ],
        )
        .unwrap_or(8.0),
        next_funding_time: datetime_field(body, &["next_funding_time"]),
        updated_at: datetime_field(body, &["updated_at"])
            .or_else(|| datetime_field(&event.payload, &["observed_at", "received_at"]))
            .unwrap_or(event.received_at),
    })
}

fn parse_symbol_precision(
    payload: &Value,
) -> Option<(ExchangeId, CanonicalSymbol, SymbolPrecision)> {
    let body = event_payload_body(payload);
    let exchange = ExchangeId::new(envelope_string_field(
        payload,
        &["exchange", "exchange_id"],
    )?);
    let symbol = parse_payload_symbol(payload).or_else(|| parse_payload_symbol(body))?;
    Some((
        exchange,
        symbol,
        SymbolPrecision {
            price_tick: f64_field(body, &["price_tick", "tick_size"]).unwrap_or_default(),
            quantity_step: f64_field(
                body,
                &[
                    "quantity_step",
                    "qty_step",
                    "step_size",
                    "quantity_step_base",
                ],
            )
            .unwrap_or_default(),
            min_quantity: f64_field(body, &["min_quantity", "min_qty", "min_quantity_base"])
                .unwrap_or_default(),
            min_notional_usdt: f64_field(body, &["min_notional_usdt", "min_notional"])
                .unwrap_or_default(),
        },
    ))
}

fn parse_fee_rates(payload: &Value) -> Option<(ExchangeId, CanonicalSymbol, FeeRates)> {
    let body = event_payload_body(payload);
    let exchange = ExchangeId::new(envelope_string_field(
        payload,
        &["exchange", "exchange_id"],
    )?);
    let symbol = parse_payload_symbol(payload).or_else(|| parse_payload_symbol(body))?;
    Some((
        exchange,
        symbol,
        FeeRates {
            maker: f64_field(body, &["maker", "maker_fee", "maker_fee_rate"])?,
            taker: f64_field(body, &["taker", "taker_fee", "taker_fee_rate"])?,
        },
    ))
}

fn parse_route_risk_snapshot(
    payload: &Value,
) -> Option<(
    Option<String>,
    Option<ExchangeId>,
    Option<CanonicalSymbol>,
    RiskSnapshot,
)> {
    let route_id = envelope_string_field(payload, &["route_id"]);
    let exchange =
        envelope_string_field(payload, &["exchange", "exchange_id"]).map(ExchangeId::new);
    let symbol = parse_payload_symbol(payload);
    let risk_payload = payload
        .get("risk")
        .unwrap_or_else(|| event_payload_body(payload));
    let unmanaged_same_symbol =
        bool_field(risk_payload, &["unmanaged_same_symbol_position"]).unwrap_or(false);
    let risk = RiskSnapshot {
        private_stream_ready: bool_field(risk_payload, &["private_stream_ready"]).unwrap_or(false),
        precision_ready: bool_field(risk_payload, &["precision_ready"]).unwrap_or(true),
        account_position_ready: bool_field(risk_payload, &["account_position_ready"])
            .unwrap_or_else(|| datetime_field(risk_payload, &["positions_observed_at"]).is_some()),
        no_unmanaged_position: bool_field(risk_payload, &["no_unmanaged_position"])
            .unwrap_or(!unmanaged_same_symbol),
        symbol_cooling_down: bool_field(risk_payload, &["symbol_cooling_down"]).unwrap_or(false),
        pending_repair: bool_field(risk_payload, &["pending_repair"]).unwrap_or(false),
        unknown_order_state: bool_field(risk_payload, &["unknown_order_state"]).unwrap_or(false),
        mmr_pct: f64_field(risk_payload, &["mmr_pct", "mmr"]).unwrap_or_default(),
        adl_pct: f64_field(risk_payload, &["adl_pct", "adl"]).unwrap_or_default(),
        liquidation_buffer_pct: f64_field(
            risk_payload,
            &["liquidation_buffer_pct", "liq_buffer_pct"],
        )
        .unwrap_or_default(),
        single_leg_exposure_ms: u64_field(risk_payload, &["single_leg_exposure_ms"])
            .unwrap_or_default(),
    };
    Some((route_id, exchange, symbol, risk))
}

fn parse_route_position(event: &AccountEvent) -> Option<RoutePosition> {
    let position_payload = event
        .payload
        .get("position")
        .unwrap_or_else(|| event_payload_body(&event.payload));
    serde_json::from_value(position_payload.clone())
        .ok()
        .or_else(|| {
            parse_route_position_contract(&event.payload, position_payload, event.received_at)
        })
}

fn parse_funding_settlement(event: &AccountEvent) -> Option<FundingSettlementUpdate> {
    let body = funding_settlement_body(&event.payload);
    let exchange = envelope_string_field(&event.payload, &["exchange", "exchange_id"])
        .or_else(|| string_field(body, &["exchange", "exchange_id"]))
        .map(ExchangeId::new);
    let canonical_symbol =
        parse_payload_symbol(&event.payload).or_else(|| parse_payload_symbol(body));
    let route_id = envelope_string_field(&event.payload, &["route_id"])
        .or_else(|| string_field(body, &["route_id"]));
    let position_side = position_side_field(&event.payload, &["position_side", "side"])
        .or_else(|| position_side_field(body, &["position_side", "side"]));
    let notional_usdt = f64_field(
        body,
        &[
            "notional_usdt",
            "settlement_notional_usdt",
            "position_notional_usdt",
        ],
    )
    .or_else(|| f64_field(&event.payload, &["notional_usdt"]));
    let funding_rate = f64_field(body, &["funding_rate", "rate"])
        .or_else(|| f64_field(&event.payload, &["funding_rate", "rate"]));
    let funding_pnl_usdt = f64_field(
        body,
        &[
            "funding_pnl_usdt",
            "pnl_usdt",
            "funding_fee_usdt",
            "settlement_pnl_usdt",
        ],
    )?;
    let mark_price = f64_field(body, &["mark_price", "settlement_mark_price"])
        .or_else(|| f64_field(&event.payload, &["mark_price"]));
    let index_price = f64_field(body, &["index_price", "settlement_index_price"])
        .or_else(|| f64_field(&event.payload, &["index_price"]));
    let settled_at = datetime_field(body, &["settled_at", "funding_time", "timestamp"])
        .or_else(|| {
            datetime_field(
                &event.payload,
                &["settled_at", "observed_at", "received_at"],
            )
        })
        .unwrap_or(event.received_at);
    let settlement_id = string_field(
        body,
        &[
            "settlement_id",
            "bundle_id",
            "idempotency_key",
            "correlation_id",
            "event_id",
        ],
    )
    .or_else(|| {
        envelope_string_field(
            &event.payload,
            &[
                "settlement_id",
                "bundle_id",
                "idempotency_key",
                "correlation_id",
                "event_id",
            ],
        )
    })
    .unwrap_or_else(|| {
        let route = route_id.as_deref().unwrap_or("unknown-route");
        let exchange = exchange
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "unknown-exchange".to_string());
        let symbol = canonical_symbol
            .as_ref()
            .map(CanonicalSymbol::as_pair)
            .unwrap_or_else(|| "unknown-symbol".to_string());
        let settled_at_ms = settled_at.timestamp_millis();
        format!("{route}:{exchange}:{symbol}:{settled_at_ms}:{funding_pnl_usdt:.12}")
    });

    Some(FundingSettlementUpdate {
        settlement_id,
        route_id,
        exchange,
        canonical_symbol,
        position_side,
        notional_usdt,
        funding_rate,
        funding_pnl_usdt,
        mark_price,
        index_price,
        settled_at,
    })
}

fn funding_settlement_body(payload: &Value) -> &Value {
    let body = event_payload_body(payload);
    if body
        .get("payload_type")
        .and_then(Value::as_str)
        .is_some_and(|payload_type| payload_type == "funding_settlement")
    {
        return body.get("payload").unwrap_or(body);
    }
    if let Some(ledger_payload) = body.get("payload") {
        if body
            .get("payload_type")
            .and_then(Value::as_str)
            .is_some_and(|payload_type| payload_type == "funding_settlement")
        {
            return ledger_payload;
        }
    }
    body
}

fn position_side_for_exchange(
    exchange: &ExchangeId,
    position: &RoutePosition,
) -> Option<PositionSide> {
    if exchange == &position.long_exchange {
        Some(PositionSide::Long)
    } else if exchange == &position.short_exchange {
        Some(PositionSide::Short)
    } else {
        None
    }
}

fn ledger_exchange_id(exchange: &ExchangeId) -> Option<LedgerExchangeId> {
    LedgerExchangeId::new(exchange.to_string()).ok()
}

fn ledger_canonical_symbol(symbol: &CanonicalSymbol) -> Option<LedgerCanonicalSymbol> {
    LedgerCanonicalSymbol::new(&symbol.base, &symbol.quote).ok()
}

fn ledger_position_side(position_side: PositionSide) -> LedgerPositionSide {
    match position_side {
        PositionSide::Long => LedgerPositionSide::Long,
        PositionSide::Short => LedgerPositionSide::Short,
    }
}

fn parse_payload_symbol(payload: &Value) -> Option<CanonicalSymbol> {
    string_field(payload, &["canonical_symbol", "symbol"]).map(|symbol| parse_event_symbol(&symbol))
}

fn parse_route_position_contract(
    envelope: &Value,
    payload: &Value,
    received_at: DateTime<Utc>,
) -> Option<RoutePosition> {
    let route_id = envelope_string_field(envelope, &["route_id"])?;
    let canonical_symbol = parse_payload_symbol(envelope)
        .or_else(|| parse_payload_symbol(payload))
        .unwrap_or_else(|| CanonicalSymbol::new("UNKNOWN", "USDT"));
    let long_exchange = ExchangeId::new(string_field(payload, &["long_exchange"])?);
    let short_exchange = ExchangeId::new(string_field(payload, &["short_exchange"])?);
    let state = route_state_from_str(&string_field(payload, &["state"])?);
    Some(RoutePosition {
        route_id,
        state,
        long_exchange,
        short_exchange,
        canonical_symbol,
        long_base_quantity: f64_field(payload, &["long_base_quantity"]).unwrap_or_default(),
        short_base_quantity: f64_field(payload, &["short_base_quantity"]).unwrap_or_default(),
        long_avg_entry_price: f64_field(payload, &["long_avg_entry_price"]).unwrap_or_default(),
        short_avg_entry_price: f64_field(payload, &["short_avg_entry_price"]).unwrap_or_default(),
        weighted_open_spread_pct: f64_field(payload, &["weighted_open_spread_pct"])
            .unwrap_or_default(),
        current_notional_usdt: f64_field(payload, &["current_notional_usdt"]).unwrap_or_default(),
        cumulative_funding_pnl_usdt: f64_field(payload, &["cumulative_funding_pnl_usdt"])
            .unwrap_or_default(),
        cumulative_fee_usdt: f64_field(payload, &["cumulative_fee_usdt"]).unwrap_or_default(),
        opened_at: datetime_field(payload, &["opened_at"]).unwrap_or(received_at),
        last_add_at: datetime_field(payload, &["last_add_at"]),
        last_add_spread_pct: f64_field(payload, &["last_add_spread_pct"]),
        open_slices: payload
            .get("open_slices")
            .and_then(|value| value.as_array().map(Vec::len))
            .or_else(|| usize_field(payload, &["open_slices_count"]))
            .unwrap_or(1),
        pending_maker_orders: usize_field(payload, &["pending_maker_orders"]).unwrap_or_default(),
        pending_hedge_orders: usize_field(payload, &["pending_hedge_orders"]).unwrap_or_default(),
        unknown_order_state: bool_field(payload, &["unknown_order_state"]).unwrap_or(false),
        pending_repair: bool_field(payload, &["pending_repair"]).unwrap_or(false),
    })
}

fn route_state_from_str(value: &str) -> RouteState {
    match value.trim().to_ascii_lowercase().as_str() {
        "maker_open_pending" => RouteState::MakerOpenPending,
        "hedge_pending" => RouteState::HedgePending,
        "open" => RouteState::Open,
        "add_pending" => RouteState::AddPending,
        "target_close_ready" => RouteState::TargetCloseReady,
        "closing" => RouteState::Closing,
        "closed" => RouteState::Closed,
        "repairing_single_leg" => RouteState::RepairingSingleLeg,
        "close_only" => RouteState::CloseOnly,
        "risk_stopped" => RouteState::RiskStopped,
        _ => RouteState::Observing,
    }
}

fn event_payload_body(payload: &Value) -> &Value {
    payload.get("payload").unwrap_or(payload)
}

fn envelope_string_field(payload: &Value, names: &[&str]) -> Option<String> {
    string_field(payload, names).or_else(|| string_field(event_payload_body(payload), names))
}

fn execution_fill_observed(payload: &Value) -> bool {
    bool_field(payload, &["filled", "is_filled"]).unwrap_or(false)
        || execution_fill_quantity(payload).is_some_and(|quantity| quantity > 0.0)
        || envelope_string_field(payload, &["status"])
            .map(|status| {
                matches!(
                    status.trim().to_ascii_lowercase().as_str(),
                    "filled" | "partially_filled" | "partial_fill"
                )
            })
            .unwrap_or(false)
}

fn execution_fill_quantity(payload: &Value) -> Option<f64> {
    let body = event_payload_body(payload);
    f64_field(
        body,
        &[
            "filled_quantity",
            "filled_quantity_base",
            "fill_quantity",
            "fill_quantity_base",
            "last_fill_quantity",
            "last_fill_quantity_base",
        ],
    )
    .or_else(|| {
        f64_field(
            payload,
            &[
                "filled_quantity",
                "filled_quantity_base",
                "fill_quantity",
                "fill_quantity_base",
            ],
        )
    })
}

fn parse_event_symbol(symbol: &str) -> CanonicalSymbol {
    let normalized = symbol.trim().to_ascii_uppercase().replace(['-', '_'], "/");
    if let Some((base, quote)) = normalized.split_once('/') {
        return CanonicalSymbol::new(base, quote);
    }
    for quote in ["USDT", "USDC", "USD"] {
        if normalized.ends_with(quote) && normalized.len() > quote.len() {
            return CanonicalSymbol::new(&normalized[..normalized.len() - quote.len()], quote);
        }
    }
    CanonicalSymbol::new(normalized, "USDT")
}

fn string_field(payload: &Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        payload.get(*name).and_then(|value| {
            value
                .as_str()
                .map(str::to_string)
                .or_else(|| value.as_f64().map(|number| number.to_string()))
        })
    })
}

fn f64_field(payload: &Value, names: &[&str]) -> Option<f64> {
    names.iter().find_map(|name| {
        payload.get(*name).and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
    })
}

fn position_side_field(payload: &Value, names: &[&str]) -> Option<PositionSide> {
    string_field(payload, names).and_then(|value| {
        match value.trim().to_ascii_lowercase().as_str() {
            "long" | "buy" | "1" => Some(PositionSide::Long),
            "short" | "sell" | "2" => Some(PositionSide::Short),
            _ => None,
        }
    })
}

fn bool_field(payload: &Value, names: &[&str]) -> Option<bool> {
    names.iter().find_map(|name| {
        payload.get(*name).and_then(|value| {
            value
                .as_bool()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
    })
}

fn usize_field(payload: &Value, names: &[&str]) -> Option<usize> {
    u64_field(payload, names).and_then(|value| usize::try_from(value).ok())
}

fn u64_field(payload: &Value, names: &[&str]) -> Option<u64> {
    names.iter().find_map(|name| {
        payload.get(*name).and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
    })
}

fn datetime_field(payload: &Value, names: &[&str]) -> Option<DateTime<Utc>> {
    names.iter().find_map(|name| {
        payload.get(*name).and_then(|value| {
            value.as_str().and_then(|text| {
                DateTime::parse_from_rfc3339(text)
                    .map(|datetime| datetime.with_timezone(&Utc))
                    .ok()
            })
        })
    })
}

fn market_key(exchange: &ExchangeId, symbol: &CanonicalSymbol) -> String {
    format!("{}:{}", exchange, symbol.as_pair())
}

fn mid_price(book: &OrderBookTop) -> f64 {
    (book.best_bid_price + book.best_ask_price) / 2.0
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

fn execution_idempotency_key(ctx: &StrategyContext, client_order_id: &str) -> String {
    format!("{}:{}:{}", ctx.strategy_id(), ctx.run_id(), client_order_id)
}

fn sdk_side(side: OrderSide) -> SdkOrderSide {
    match side {
        OrderSide::Buy => SdkOrderSide::Buy,
        OrderSide::Sell => SdkOrderSide::Sell,
    }
}

fn sdk_time_in_force(time_in_force: TimeInForce) -> SdkTimeInForce {
    match time_in_force {
        TimeInForce::PostOnly => SdkTimeInForce::PostOnly,
        TimeInForce::ImmediateOrCancel => SdkTimeInForce::ImmediateOrCancel,
        TimeInForce::FillOrKill => SdkTimeInForce::FillOrKill,
    }
}

fn trim_float(value: f64) -> String {
    let mut text = format!("{value:.12}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rustcta_strategy_sdk::{
        ExecutionCancelCommand, ExecutionIntent, SdkResult, StrategyExecutionClient, TimerEvent,
    };
    use std::sync::{Arc, Mutex};

    struct NoopExecutionClient;

    #[async_trait]
    impl StrategyExecutionClient for NoopExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<rustcta_strategy_sdk::ExecutionOrderAck> {
            Ok(rustcta_strategy_sdk::ExecutionOrderAck {
                schema_version: 1,
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
        ) -> SdkResult<rustcta_strategy_sdk::ExecutionCancelAck> {
            Ok(rustcta_strategy_sdk::ExecutionCancelAck {
                schema_version: 1,
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
        ) -> SdkResult<rustcta_strategy_sdk::ExecutionIntentAck> {
            Ok(rustcta_strategy_sdk::ExecutionIntentAck {
                schema_version: 1,
                accepted: true,
                intent_kind: intent.intent_kind,
                reason: None,
                received_at: Utc::now(),
                payload: json!({}),
            })
        }
    }

    #[derive(Default)]
    struct RecordingExecutionClient {
        orders: Mutex<Vec<ExecutionOrderCommand>>,
        cancels: Mutex<Vec<ExecutionCancelCommand>>,
    }

    impl RecordingExecutionClient {
        fn orders(&self) -> Vec<ExecutionOrderCommand> {
            self.orders.lock().expect("orders mutex poisoned").clone()
        }

        fn cancels(&self) -> Vec<ExecutionCancelCommand> {
            self.cancels.lock().expect("cancels mutex poisoned").clone()
        }
    }

    #[async_trait]
    impl StrategyExecutionClient for RecordingExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<rustcta_strategy_sdk::ExecutionOrderAck> {
            self.orders
                .lock()
                .expect("orders mutex poisoned")
                .push(command.clone());
            Ok(rustcta_strategy_sdk::ExecutionOrderAck {
                schema_version: 1,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: Some("exec-1".to_string()),
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn cancel_order(
            &self,
            command: ExecutionCancelCommand,
        ) -> SdkResult<rustcta_strategy_sdk::ExecutionCancelAck> {
            self.cancels
                .lock()
                .expect("cancels mutex poisoned")
                .push(command.clone());
            Ok(rustcta_strategy_sdk::ExecutionCancelAck {
                schema_version: 1,
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
        ) -> SdkResult<rustcta_strategy_sdk::ExecutionIntentAck> {
            Ok(rustcta_strategy_sdk::ExecutionIntentAck {
                schema_version: 1,
                accepted: true,
                intent_kind: intent.intent_kind,
                reason: None,
                received_at: Utc::now(),
                payload: json!({}),
            })
        }
    }

    fn ctx() -> StrategyContext {
        StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            serde_json::to_value(FundingSpreadExpansionMakerConfig::default()).unwrap(),
            Arc::new(NoopExecutionClient),
        )
    }

    fn ctx_with_config(
        config: FundingSpreadExpansionMakerConfig,
        execution: Arc<dyn StrategyExecutionClient>,
    ) -> StrategyContext {
        StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            serde_json::to_value(config).unwrap(),
            execution,
        )
    }

    fn runtime_config(live: bool) -> FundingSpreadExpansionMakerConfig {
        let mut config = FundingSpreadExpansionMakerConfig::default();
        config.mode = if live { "live" } else { "observe" }.to_string();
        config.dry_run = !live;
        config.thresholds.min_expected_total_edge_pct = -0.01;
        config.funding.max_funding_snapshot_age_ms = 10_000;
        config.risk.max_book_age_ms = 10_000;
        config
    }

    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-12T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn market_event(exchange: &str, _kind: &str, payload: Value) -> StrategyEvent {
        StrategyEvent::MarketData(MarketDataEvent {
            schema_version: 1,
            exchange_id: exchange.to_string(),
            symbol: "HUSDT/USDT".to_string(),
            received_at: fixed_now(),
            payload,
        })
    }

    fn market_envelope_event(exchange: &str, event_type: &str, payload: Value) -> StrategyEvent {
        StrategyEvent::MarketData(MarketDataEvent {
            schema_version: 1,
            exchange_id: exchange.to_string(),
            symbol: "HUSDT/USDT".to_string(),
            received_at: fixed_now(),
            payload: json!({
                "schema_version": "funding_spread_expansion_maker.runtime.v1",
                "event_type": event_type,
                "event_id": format!("{exchange}-{event_type}"),
                "source": "test_provider",
                "exchange": exchange,
                "canonical_symbol": "HUSDT/USDT",
                "observed_at": fixed_now().to_rfc3339(),
                "received_at": fixed_now().to_rfc3339(),
                "payload": payload
            }),
        })
    }

    #[test]
    fn funding_snapshot_parser_should_keep_scanner_enrichment_fields() {
        let StrategyEvent::MarketData(event) = market_envelope_event(
            "bybit",
            "funding_rate",
            json!({
                "funding_rate": "0.0001",
                "predicted_funding_rate": "0.0002",
                "mark_price": "65000.5",
                "index_price": "64999.9",
                "open_interest": "12345",
                "turnover_24h": "1000000",
                "volume_24h": "100",
                "next_funding_time": (fixed_now() + Duration::hours(4)).to_rfc3339()
            }),
        ) else {
            panic!("expected market data event");
        };

        let snapshot = parse_funding_snapshot(&event).expect("funding snapshot");

        assert_eq!(snapshot.exchange, ExchangeId::new("bybit"));
        assert_eq!(snapshot.funding_rate, 0.0001);
        assert_eq!(snapshot.predicted_funding_rate, Some(0.0002));
        assert_eq!(snapshot.mark_price, Some(65000.5));
        assert_eq!(snapshot.index_price, Some(64999.9));
        assert_eq!(snapshot.open_interest, Some(12345.0));
        assert_eq!(snapshot.turnover_24h, Some(1_000_000.0));
        assert_eq!(snapshot.volume_24h, Some(100.0));
        assert!(snapshot.next_funding_time.is_some());
    }

    fn account_event(kind: &str, payload: Value) -> StrategyEvent {
        let mut payload = payload;
        payload["event_kind"] = json!(kind);
        StrategyEvent::Account(AccountEvent {
            schema_version: 1,
            account_id: "account-1".to_string(),
            received_at: fixed_now(),
            payload,
        })
    }

    fn account_envelope_event(exchange: &str, event_type: &str, payload: Value) -> StrategyEvent {
        StrategyEvent::Account(AccountEvent {
            schema_version: 1,
            account_id: "account-1".to_string(),
            received_at: fixed_now(),
            payload: json!({
                "schema_version": "funding_spread_expansion_maker.runtime.v1",
                "event_type": event_type,
                "event_id": format!("{exchange}-{event_type}"),
                "source": "test_provider",
                "exchange": exchange,
                "canonical_symbol": "HUSDT/USDT",
                "observed_at": fixed_now().to_rfc3339(),
                "received_at": fixed_now().to_rfc3339(),
                "payload": payload
            }),
        })
    }

    fn execution_event(client_order_id: String, payload: Value) -> StrategyEvent {
        StrategyEvent::Execution(ExecutionEvent {
            schema_version: 1,
            event_id: format!("exec-{client_order_id}"),
            client_order_id: Some(client_order_id),
            occurred_at: fixed_now() + Duration::milliseconds(100),
            payload,
        })
    }

    fn timer_event(fired_at: DateTime<Utc>) -> StrategyEvent {
        StrategyEvent::Timer(TimerEvent {
            schema_version: 1,
            timer_id: "test-timer".to_string(),
            fired_at,
            payload: json!({}),
        })
    }

    async fn feed_open_ready_state(runtime: &mut FundingSpreadExpansionMakerRuntime) {
        runtime
            .handle_event(market_event(
                "mexc",
                "order_book_top",
                json!({
                    "event_kind": "order_book_top",
                    "best_bid_price": 0.1557,
                    "best_bid_quantity": 1000.0,
                    "best_ask_price": 0.1559,
                    "best_ask_quantity": 1000.0,
                    "levels": 1
                }),
            ))
            .await
            .unwrap();
        runtime
            .handle_event(market_event(
                "bybit",
                "order_book_top",
                json!({
                    "event_kind": "order_book_top",
                    "best_bid_price": 0.1485,
                    "best_bid_quantity": 1000.0,
                    "best_ask_price": 0.1487,
                    "best_ask_quantity": 1000.0,
                    "levels": 1
                }),
            ))
            .await
            .unwrap();
        runtime
            .handle_event(market_event(
                "mexc",
                "funding_rate",
                json!({
                    "event_kind": "funding_rate",
                    "funding_rate": -0.015,
                    "mark_price": 0.1558,
                    "next_funding_time": (fixed_now() + Duration::hours(4)).to_rfc3339(),
                    "updated_at": fixed_now().to_rfc3339()
                }),
            ))
            .await
            .unwrap();
        runtime
            .handle_event(market_event(
                "bybit",
                "funding_rate",
                json!({
                    "event_kind": "funding_rate",
                    "funding_rate": 0.015,
                    "mark_price": 0.1486,
                    "next_funding_time": (fixed_now() + Duration::hours(4)).to_rfc3339(),
                    "updated_at": fixed_now().to_rfc3339()
                }),
            ))
            .await
            .unwrap();
        for exchange in ["mexc", "bybit"] {
            runtime
                .handle_event(account_event(
                    "symbol_precision",
                    json!({
                        "exchange_id": exchange,
                        "symbol": "HUSDT/USDT",
                        "price_tick": 0.0001,
                        "quantity_step": 0.1,
                        "min_quantity": 0.1,
                        "min_notional_usdt": 5.0
                    }),
                ))
                .await
                .unwrap();
            runtime
                .handle_event(account_event(
                    "fee_rates",
                    json!({
                        "exchange_id": exchange,
                        "symbol": "HUSDT/USDT",
                        "maker": 0.0002,
                        "taker": 0.0005
                    }),
                ))
                .await
                .unwrap();
        }
        runtime
            .handle_event(account_event(
                "risk_snapshot",
                json!({
                    "route_id": "default_route",
                    "private_stream_ready": true,
                    "precision_ready": true,
                    "account_position_ready": true,
                    "no_unmanaged_position": true,
                    "symbol_cooling_down": false,
                    "mmr_pct": 5.0,
                    "adl_pct": 10.0,
                    "liquidation_buffer_pct": 100.0
                }),
            ))
            .await
            .unwrap();
    }

    async fn feed_envelope_open_ready_state(runtime: &mut FundingSpreadExpansionMakerRuntime) {
        runtime
            .handle_event(market_envelope_event(
                "mexc",
                "order_book_top",
                json!({
                    "bid_price": 0.1557,
                    "bid_quantity_base": 1000.0,
                    "ask_price": 0.1559,
                    "ask_quantity_base": 1000.0
                }),
            ))
            .await
            .unwrap();
        runtime
            .handle_event(market_envelope_event(
                "bybit",
                "order_book_top",
                json!({
                    "bid_price": 0.1485,
                    "bid_quantity_base": 1000.0,
                    "ask_price": 0.1487,
                    "ask_quantity_base": 1000.0
                }),
            ))
            .await
            .unwrap();
        runtime
            .handle_event(market_envelope_event(
                "mexc",
                "funding_rate",
                json!({
                    "funding_rate": -0.015,
                    "mark_price": 0.1558,
                    "next_funding_time": (fixed_now() + Duration::hours(4)).to_rfc3339()
                }),
            ))
            .await
            .unwrap();
        runtime
            .handle_event(market_envelope_event(
                "bybit",
                "funding_rate",
                json!({
                    "funding_rate": 0.015,
                    "mark_price": 0.1486,
                    "next_funding_time": (fixed_now() + Duration::hours(4)).to_rfc3339()
                }),
            ))
            .await
            .unwrap();
        for exchange in ["mexc", "bybit"] {
            runtime
                .handle_event(account_envelope_event(
                    exchange,
                    "symbol_precision",
                    json!({
                        "price_tick": 0.0001,
                        "quantity_step_base": 0.1,
                        "min_quantity_base": 0.1,
                        "min_notional_usdt": 5.0,
                        "contract_size_base": 1.0,
                        "quantity_mode": "base"
                    }),
                ))
                .await
                .unwrap();
            runtime
                .handle_event(account_envelope_event(
                    exchange,
                    "fee_rates",
                    json!({
                        "maker_fee_rate": 0.0002,
                        "taker_fee_rate": 0.0005,
                        "fee_currency": "USDT"
                    }),
                ))
                .await
                .unwrap();
        }
        runtime
            .handle_event(account_envelope_event(
                "mexc",
                "risk_snapshot",
                json!({
                    "private_stream_ready": true,
                    "positions_observed_at": fixed_now().to_rfc3339(),
                    "unmanaged_same_symbol_position": false,
                    "symbol_cooling_down": false,
                    "mmr_pct": 5.0,
                    "adl_pct": 10.0,
                    "liquidation_buffer_pct": 100.0
                }),
            ))
            .await
            .unwrap();
    }

    #[test]
    fn strategy_spec_should_expose_adapter_free_contract() {
        let spec = strategy_spec();
        assert_eq!(spec.strategy_kind, STRATEGY_KIND);
        assert!(spec
            .required_account_permissions
            .iter()
            .any(|permission| permission.permission == AccountPermission::TradePerpetual));
        assert!(spec
            .risk_capabilities
            .iter()
            .any(|capability| capability.capability == RiskCapability::ReduceOnlyOrders));
    }

    #[tokio::test]
    async fn runtime_snapshot_should_be_secret_free() {
        let mut runtime = FundingSpreadExpansionMakerRuntime::new();
        runtime.start(ctx()).await.unwrap();
        let snapshot = runtime.snapshot().await.unwrap();
        let text = serde_json::to_string(&snapshot.payload).unwrap();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("token"));
    }

    #[test]
    fn order_command_should_encode_reduce_only_and_post_only_metadata() {
        let draft = OrderDraft {
            exchange: ExchangeId::new("bybit"),
            canonical_symbol: CanonicalSymbol::new("HUSDT", "USDT"),
            side: OrderSide::Buy,
            position_side: PositionSide::Short,
            base_quantity: 54.0,
            order_quantity: 54.0,
            reference_price: 0.1486,
            limit_price: 0.149,
            post_only: true,
            reduce_only: true,
            time_in_force: TimeInForce::PostOnly,
            intent_kind: OrderIntentKind::CloseMaker,
        };
        let command =
            order_draft_to_command(&ctx(), &draft, "risk-1", "cid-1".to_string(), Utc::now());
        assert!(command.reduce_only);
        assert_eq!(command.time_in_force, Some(SdkTimeInForce::PostOnly));
        assert_eq!(command.metadata["post_only"], json!(true));
    }

    #[tokio::test]
    async fn dry_run_runtime_should_evaluate_without_submitting_orders() {
        let execution = Arc::new(RecordingExecutionClient::default());
        let mut runtime = FundingSpreadExpansionMakerRuntime::new();
        runtime
            .start(ctx_with_config(runtime_config(false), execution.clone()))
            .await
            .unwrap();

        feed_open_ready_state(&mut runtime).await;

        assert!(execution.orders().is_empty());
        let snapshot = runtime.snapshot().await.unwrap();
        assert_eq!(snapshot.payload["submitted_order_count"], json!(0));
        assert_eq!(
            snapshot.payload["route_runtime"][0]["last_open_reject_reason"],
            Value::Null
        );
    }

    #[tokio::test]
    async fn live_runtime_should_stage_hedge_until_maker_fill() {
        let execution = Arc::new(RecordingExecutionClient::default());
        let mut runtime = FundingSpreadExpansionMakerRuntime::new();
        runtime
            .start(ctx_with_config(runtime_config(true), execution.clone()))
            .await
            .unwrap();

        feed_open_ready_state(&mut runtime).await;

        let orders = execution.orders();
        assert_eq!(orders.len(), 1);
        assert_eq!(
            orders[0].metadata["intent_kind"],
            json!(OrderIntentKind::OpenMaker)
        );
        assert_eq!(orders[0].time_in_force, Some(SdkTimeInForce::PostOnly));
        assert!(!orders[0].reduce_only);
        let maker_client_order_id = orders[0].client_order_id.clone();

        runtime
            .handle_event(execution_event(
                maker_client_order_id,
                json!({
                    "route_id": "default_route",
                    "status": "filled",
                    "filled_quantity_base": orders[0].quantity.parse::<f64>().unwrap(),
                    "terminal": true
                }),
            ))
            .await
            .unwrap();

        let orders = execution.orders();
        assert_eq!(orders.len(), 2);
        assert_eq!(
            orders[1].metadata["intent_kind"],
            json!(OrderIntentKind::OpenHedgeTaker)
        );
        assert_eq!(
            orders[1].time_in_force,
            Some(SdkTimeInForce::ImmediateOrCancel)
        );
        assert_eq!(orders[1].metadata["route_action"], json!("open_hedge"));
        assert!(orders.iter().all(|order| !order.reduce_only));
    }

    #[tokio::test]
    async fn runtime_should_accept_documented_envelope_events() {
        let execution = Arc::new(RecordingExecutionClient::default());
        let mut runtime = FundingSpreadExpansionMakerRuntime::new();
        runtime
            .start(ctx_with_config(runtime_config(true), execution.clone()))
            .await
            .unwrap();

        feed_envelope_open_ready_state(&mut runtime).await;

        let orders = execution.orders();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange_id, "mexc");
        assert_eq!(orders[0].symbol, "HUSDT/USDT");
        assert_eq!(
            orders[0].metadata["intent_kind"],
            json!(OrderIntentKind::OpenMaker)
        );
    }

    #[tokio::test]
    async fn runtime_should_accept_scanner_contract_for_all_target_contract_venues() {
        let execution = Arc::new(RecordingExecutionClient::default());
        let mut config = runtime_config(true);
        config.routes = vec![
            RouteConfig {
                route_id: "aster_mexc".to_string(),
                leg_a_exchange: "aster".to_string(),
                leg_b_exchange: "mexc".to_string(),
                ..RouteConfig::default()
            },
            RouteConfig {
                route_id: "kucoin_bybit".to_string(),
                leg_a_exchange: "kucoinfutures".to_string(),
                leg_b_exchange: "bybit".to_string(),
                ..RouteConfig::default()
            },
        ];
        let mut runtime = FundingSpreadExpansionMakerRuntime::new();
        runtime
            .start(ctx_with_config(config, execution))
            .await
            .unwrap();

        for (exchange, bid, ask, funding_rate) in [
            ("aster", 0.1560, 0.1562, -0.012),
            ("mexc", 0.1557, 0.1559, 0.011),
            ("kucoinfutures", 0.1488, 0.1490, -0.010),
            ("bybit", 0.1485, 0.1487, 0.012),
        ] {
            runtime
                .handle_event(market_envelope_event(
                    exchange,
                    "order_book_top",
                    json!({
                        "bid_price": bid,
                        "bid_quantity_base": 1000.0,
                        "ask_price": ask,
                        "ask_quantity_base": 1000.0,
                        "levels": 5
                    }),
                ))
                .await
                .unwrap();
            runtime
                .handle_event(market_envelope_event(
                    exchange,
                    "funding_rate",
                    json!({
                        "funding_rate": funding_rate,
                        "predicted_funding_rate": funding_rate * 0.9,
                        "mark_price": (bid + ask) / 2.0,
                        "index_price": (bid + ask) / 2.0,
                        "open_interest": 12345.0,
                        "turnover_24h": 1_000_000.0,
                        "volume_24h": 100.0,
                        "next_funding_time": (fixed_now() + Duration::hours(4)).to_rfc3339()
                    }),
                ))
                .await
                .unwrap();
            runtime
                .handle_event(account_envelope_event(
                    exchange,
                    "symbol_precision",
                    json!({
                        "price_tick": 0.0001,
                        "quantity_step_base": 0.1,
                        "min_quantity_base": 0.1,
                        "min_notional_usdt": 5.0
                    }),
                ))
                .await
                .unwrap();
            runtime
                .handle_event(account_envelope_event(
                    exchange,
                    "fee_rates",
                    json!({
                        "maker_fee_rate": 0.0002,
                        "taker_fee_rate": 0.0005,
                        "fee_currency": "USDT"
                    }),
                ))
                .await
                .unwrap();
        }
        for route_id in ["aster_mexc", "kucoin_bybit"] {
            runtime
                .handle_event(account_event(
                    "risk_snapshot",
                    json!({
                        "route_id": route_id,
                        "private_stream_ready": true,
                        "precision_ready": true,
                        "account_position_ready": true,
                        "no_unmanaged_position": true,
                        "symbol_cooling_down": false,
                        "mmr_pct": 5.0,
                        "adl_pct": 10.0,
                        "liquidation_buffer_pct": 100.0
                    }),
                ))
                .await
                .unwrap();
        }

        assert_eq!(runtime.state.order_books.len(), 4);
        assert_eq!(runtime.state.funding.len(), 4);
        assert_eq!(runtime.state.precision.len(), 4);
        assert_eq!(runtime.state.fees.len(), 4);
        for route_id in ["aster_mexc", "kucoin_bybit"] {
            let evaluation = runtime
                .state
                .last_open_evaluations
                .get(route_id)
                .expect("route should evaluate after scanner and account events");
            assert_ne!(
                evaluation.reject_reason,
                Some(EvaluationRejectReason::FundingMarkPriceMissing)
            );
            assert_ne!(
                evaluation.reject_reason,
                Some(EvaluationRejectReason::FundingNextTimeMissing)
            );
            assert_ne!(
                evaluation.reject_reason,
                Some(EvaluationRejectReason::PrecisionNotReady)
            );
        }
    }

    #[test]
    fn account_settlement_events_should_update_route_funding_pnl_once() {
        let mut runtime = FundingSpreadExpansionMakerRuntime::new();
        let mut position = RoutePosition::hedged_open("default_route", fixed_now());
        position.long_exchange = ExchangeId::new("mexc");
        position.short_exchange = ExchangeId::new("bybit");
        position.canonical_symbol = CanonicalSymbol::new("HUSDT", "USDT");
        position.current_notional_usdt = 100.0;
        runtime
            .state
            .positions
            .insert(position.route_id.clone(), position);

        let StrategyEvent::Account(first) = account_envelope_event(
            "mexc",
            "funding_settlement",
            json!({
                "bundle_id": "settlement-1",
                "route_id": "default_route",
                "exchange_id": "mexc",
                "canonical_symbol": "HUSDT/USDT",
                "position_side": "long",
                "notional_usdt": 100.0,
                "funding_rate": 0.00018,
                "funding_pnl_usdt": 0.018,
                "mark_price": 0.155,
                "settled_at": fixed_now().to_rfc3339()
            }),
        ) else {
            panic!("expected account event");
        };
        runtime.ingest_account_event(&first);
        runtime.ingest_account_event(&first);

        let StrategyEvent::Account(second) = account_event(
            "funding_settlement_event",
            json!({
                "payload_type": "funding_settlement",
                "payload": {
                    "bundle_id": "settlement-2",
                    "exchange_id": "bybit",
                    "canonical_symbol": "HUSDT/USDT",
                    "position_side": "short",
                    "notional_usdt": 100.0,
                    "funding_rate": -0.00003,
                    "funding_pnl_usdt": -0.003,
                    "settled_at": fixed_now().to_rfc3339()
                }
            }),
        ) else {
            panic!("expected account event");
        };
        runtime.ingest_account_event(&second);

        let pnl = runtime
            .state
            .positions
            .get("default_route")
            .expect("position")
            .cumulative_funding_pnl_usdt;
        assert!((pnl - 0.015).abs() < f64::EPSILON);

        assert_eq!(runtime.state.funding_settlement_ledger_events.len(), 2);
        let rustcta_event_ledger::LedgerPayload::FundingSettlement(first_record) =
            &runtime.state.funding_settlement_ledger_events[0].payload
        else {
            panic!("expected funding settlement ledger event");
        };
        assert_eq!(first_record.bundle_id, "settlement-1");
        assert_eq!(first_record.exchange_id.as_str(), "mexc");
        assert_eq!(first_record.canonical_symbol.as_str(), "HUSDT/USDT");
        assert_eq!(first_record.position_side, LedgerPositionSide::Long);
        assert_eq!(first_record.mark_price, Some(0.155));
        first_record.validated().expect("valid settlement record");

        let rustcta_event_ledger::LedgerPayload::FundingSettlement(second_record) =
            &runtime.state.funding_settlement_ledger_events[1].payload
        else {
            panic!("expected funding settlement ledger event");
        };
        assert_eq!(second_record.bundle_id, "settlement-2");
        assert_eq!(second_record.exchange_id.as_str(), "bybit");
        assert_eq!(second_record.position_side, LedgerPositionSide::Short);
        second_record.validated().expect("valid settlement record");
    }

    #[tokio::test]
    async fn runtime_should_cancel_stale_maker_and_drop_staged_hedge() {
        let execution = Arc::new(RecordingExecutionClient::default());
        let mut runtime = FundingSpreadExpansionMakerRuntime::new();
        let config = runtime_config(true);
        let timeout_ms = config.execution.maker_order_timeout_ms;
        runtime
            .start(ctx_with_config(config, execution.clone()))
            .await
            .unwrap();

        feed_open_ready_state(&mut runtime).await;
        assert_eq!(execution.orders().len(), 1);

        runtime
            .handle_event(timer_event(
                fixed_now() + Duration::milliseconds(i64::try_from(timeout_ms).unwrap() + 1),
            ))
            .await
            .unwrap();

        let cancels = execution.cancels();
        assert_eq!(cancels.len(), 1);
        assert_eq!(cancels[0].metadata["route_id"], json!("default_route"));
        assert_eq!(
            cancels[0].metadata["cancel_reason"],
            json!("maker_timeout_or_signal_invalid")
        );
        let snapshot = runtime.snapshot().await.unwrap();
        assert_eq!(snapshot.payload["canceled_order_count"], json!(1));
        assert_eq!(snapshot.payload["pending_order_count"], json!(0));
    }

    #[tokio::test]
    async fn live_runtime_should_submit_reduce_only_close_orders_at_target_spread() {
        let execution = Arc::new(RecordingExecutionClient::default());
        let mut runtime = FundingSpreadExpansionMakerRuntime::new();
        runtime
            .start(ctx_with_config(runtime_config(true), execution.clone()))
            .await
            .unwrap();

        feed_open_ready_state(&mut runtime).await;
        runtime.state.positions.clear();
        runtime.state.submitted_route_actions.clear();
        runtime.state.pending_orders.clear();
        runtime.state.staged_hedges.clear();
        execution
            .orders
            .lock()
            .expect("orders mutex poisoned")
            .clear();

        let mut position = RoutePosition::hedged_open("default_route", fixed_now());
        position.long_exchange = ExchangeId::new("mexc");
        position.short_exchange = ExchangeId::new("bybit");
        position.canonical_symbol = CanonicalSymbol::new("HUSDT", "USDT");
        position.long_base_quantity = 54.0;
        position.short_base_quantity = 54.0;
        position.long_avg_entry_price = 0.1558;
        position.short_avg_entry_price = 0.1486;
        position.weighted_open_spread_pct = -0.0469;
        position.current_notional_usdt = 8.0;
        runtime
            .handle_event(account_event(
                "route_position",
                json!({ "position": position }),
            ))
            .await
            .unwrap();
        runtime.state.submitted_route_actions.clear();
        runtime
            .handle_event(market_event(
                "bybit",
                "order_book_top",
                json!({
                    "event_kind": "order_book_top",
                    "best_bid_price": 0.1431,
                    "best_bid_quantity": 1000.0,
                    "best_ask_price": 0.1433,
                    "best_ask_quantity": 1000.0,
                    "levels": 1
                }),
            ))
            .await
            .unwrap();

        let orders = execution.orders();
        assert_eq!(orders.len(), 2);
        assert!(orders.iter().all(|order| order.reduce_only));
        assert!(orders.iter().all(|order| {
            order.metadata["intent_kind"] == json!(OrderIntentKind::CloseMaker)
                && order.time_in_force == Some(SdkTimeInForce::PostOnly)
        }));
    }
}
