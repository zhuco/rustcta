//! Live execution bridge for cross-exchange arbitrage signals and private fills.

use super::{
    ArbSignal, ArbSignalAction, CrossArbRuntime, CrossArbRuntimeState, Opportunity,
    SimulatedBundleState, SimulatedBundleStatus, StorageSink,
};
use crate::execution::{
    deterministic_client_order_id, ArbitrageBundle, BundleLeg, BundleStatus, CancelAck,
    CancelCommand, EngineDecision, ExecutionAction, ExecutionEngine, ExecutionLedger,
    ExecutionRequest, FillEvent, MakerFill, OrderCommand, OrderSide, PrivateEvent,
    PrivateEventKind,
};
use crate::market::{exchange_symbol_for, ExchangeId, ExchangeSymbol};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackedCrossArbOrder {
    pub bundle_id: String,
    pub leg: BundleLeg,
    pub exchange: ExchangeId,
    pub exchange_symbol: ExchangeSymbol,
    pub maker_side: OrderSide,
    pub taker_exchange: ExchangeId,
    pub taker_exchange_symbol: ExchangeSymbol,
    pub max_slippage_pct: Option<f64>,
    pub mode: crate::market::RuntimeMode,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossArbOrderIndex {
    by_client_order_id: HashMap<String, TrackedCrossArbOrder>,
    by_exchange_order_id: HashMap<(ExchangeId, String), TrackedCrossArbOrder>,
}

impl CrossArbOrderIndex {
    pub fn record_command(&mut self, command: &OrderCommand, tracked: TrackedCrossArbOrder) {
        self.by_client_order_id
            .insert(normalize_client_order_id(&command.client_order_id), tracked);
    }

    pub fn record_exchange_order_id(
        &mut self,
        exchange: ExchangeId,
        exchange_order_id: impl Into<String>,
        tracked: TrackedCrossArbOrder,
    ) {
        self.by_exchange_order_id
            .insert((exchange, exchange_order_id.into()), tracked);
    }

    pub fn resolve_fill(&self, fill: &FillEvent) -> Option<&TrackedCrossArbOrder> {
        fill.client_order_id
            .as_deref()
            .and_then(|client_order_id| {
                self.by_client_order_id
                    .get(&normalize_client_order_id(client_order_id))
            })
            .or_else(|| {
                fill.exchange_order_id
                    .as_ref()
                    .and_then(|exchange_order_id| {
                        self.by_exchange_order_id
                            .get(&(fill.exchange.clone(), exchange_order_id.clone()))
                    })
            })
    }
}

pub struct CrossArbExecutionCoordinator {
    engine: ExecutionEngine,
    ledger: ExecutionLedger,
    order_index: CrossArbOrderIndex,
    pending_maker_orders: HashMap<String, OrderCommand>,
}

impl CrossArbExecutionCoordinator {
    pub fn new(engine: ExecutionEngine) -> Self {
        Self {
            engine,
            ledger: ExecutionLedger::default(),
            order_index: CrossArbOrderIndex::default(),
            pending_maker_orders: HashMap::new(),
        }
    }

    pub fn ledger(&self) -> &ExecutionLedger {
        &self.ledger
    }

    pub fn order_index(&self) -> &CrossArbOrderIndex {
        &self.order_index
    }

    pub fn tracked_order_for_fill(&self, fill: &FillEvent) -> Option<TrackedCrossArbOrder> {
        self.order_index.resolve_fill(fill).cloned()
    }

    pub fn pending_maker_count(&self) -> usize {
        self.pending_maker_orders.len()
    }

    pub fn prepare_open_request(
        &mut self,
        runtime: &mut CrossArbRuntime,
        signal: &ArbSignal,
    ) -> Result<Option<ExecutionRequest>> {
        if signal.action != ArbSignalAction::Open {
            return Ok(None);
        }
        let opportunity_id = signal
            .opportunity_id
            .as_ref()
            .ok_or_else(|| anyhow!("open signal {} has no opportunity_id", signal.signal_id))?;
        let opportunity = runtime
            .state
            .opportunities
            .iter()
            .find(|item| item.opportunity_id == *opportunity_id)
            .cloned()
            .ok_or_else(|| anyhow!("opportunity {opportunity_id} not found for open signal"))?;
        let request = execution_request_from_signal(&runtime.state, signal, &opportunity)?;
        register_open_bundle(runtime, signal, &opportunity, &request)?;
        Ok(Some(request))
    }

    pub async fn execute_open_signal(
        &mut self,
        runtime: &mut CrossArbRuntime,
        signal: &ArbSignal,
    ) -> Result<Option<EngineDecision>> {
        let Some(request) = self.prepare_open_request(runtime, signal)? else {
            return Ok(None);
        };
        let decision = self.engine.execute_request(request.clone()).await;
        self.record_decision_with(&decision, |command| {
            tracked_order_for_request(command, &request)
        });
        update_runtime_after_open_decision(runtime, &decision);
        Ok(Some(decision))
    }

    pub fn hedge_candidate_from_private_event(
        &self,
        runtime: &mut CrossArbRuntime,
        event: &PrivateEvent,
    ) -> Option<MakerFill> {
        let PrivateEventKind::Fill(fill) = &event.kind else {
            return None;
        };
        maker_fill_from_tracked_private_event(runtime, self.order_index.resolve_fill(fill)?, event)
    }

    pub async fn execute_hedge_for_maker_fill(&mut self, fill: MakerFill) -> EngineDecision {
        self.pending_maker_orders.remove(&fill.bundle_id);
        let decision = self.engine.execute_hedge_for_maker_fill(fill).await;
        self.record_decision_with(&decision, tracked_order);
        decision
    }

    pub async fn cancel_expired_maker_orders(
        &mut self,
        runtime: &mut CrossArbRuntime,
        now: DateTime<Utc>,
    ) -> Vec<CancelAck> {
        let ttl_ms = runtime.state.config.execution.maker_order_ttl_ms.max(1) as i64;
        let expired = self
            .pending_maker_orders
            .iter()
            .filter_map(|(bundle_id, command)| {
                let age_ms = now
                    .signed_duration_since(command.created_at)
                    .num_milliseconds();
                let still_pending = runtime
                    .state
                    .open_bundles
                    .get(bundle_id)
                    .map(|bundle| bundle.status == SimulatedBundleStatus::MakerPending)
                    .unwrap_or(false);
                (still_pending && age_ms >= ttl_ms).then(|| (bundle_id.clone(), command.clone()))
            })
            .collect::<Vec<_>>();

        let mut acks = Vec::new();
        for (bundle_id, command) in expired {
            let cancel = CancelCommand {
                exchange: command.exchange.clone(),
                canonical_symbol: command.canonical_symbol.clone(),
                exchange_symbol: command.exchange_symbol.clone(),
                client_order_id: Some(command.client_order_id.clone()),
                exchange_order_id: None,
                reason: Some("maker TTL expired".to_string()),
                requested_at: now,
            };
            match self.engine.router().route_cancel(cancel).await {
                Ok(ack) => {
                    self.pending_maker_orders.remove(&bundle_id);
                    if let Some(bundle) = runtime.state.open_bundles.get_mut(&bundle_id) {
                        bundle.status = SimulatedBundleStatus::RiskStopped;
                        bundle.updated_at = now;
                    }
                    let _ = runtime.state.position_manager.mark_bundle_status(
                        &bundle_id,
                        BundleStatus::RiskStopped,
                        now,
                    );
                    runtime.state.risk_events.push(super::RiskEventReadModel {
                        event_id: format!("maker-ttl-cancel-{}", now.timestamp_millis()),
                        canonical_symbol: Some(command.canonical_symbol.clone()),
                        exchange: Some(command.exchange.clone()),
                        reason: super::RejectReason::RouteUnhealthy,
                        message: format!(
                            "maker order {} cancelled after TTL {}ms",
                            command.client_order_id, ttl_ms
                        ),
                        created_at: now,
                    });
                    acks.push(ack);
                }
                Err(error) => {
                    runtime.state.risk_events.push(super::RiskEventReadModel {
                        event_id: format!("maker-ttl-cancel-failed-{}", now.timestamp_millis()),
                        canonical_symbol: Some(command.canonical_symbol.clone()),
                        exchange: Some(command.exchange.clone()),
                        reason: super::RejectReason::RouteUnhealthy,
                        message: format!(
                            "maker order {} TTL cancel failed: {error}",
                            command.client_order_id
                        ),
                        created_at: now,
                    });
                }
            }
        }
        acks
    }

    fn record_decision_with(
        &mut self,
        decision: &EngineDecision,
        tracker: impl Fn(&OrderCommand) -> TrackedCrossArbOrder,
    ) {
        for command in &decision.plan.commands {
            self.ledger
                .record_order(command.clone(), decision.plan.created_at);
            self.order_index.record_command(command, tracker(command));
            if command_leg(command) == BundleLeg::Maker {
                self.pending_maker_orders
                    .insert(command.bundle_id.clone(), command.clone());
            }
        }
        for ack in &decision.submitted_orders {
            if let Some(exchange_order_id) = &ack.exchange_order_id {
                if let Some(command) = decision
                    .plan
                    .commands
                    .iter()
                    .find(|command| command.client_order_id == ack.client_order_id)
                {
                    self.order_index.record_exchange_order_id(
                        ack.exchange.clone(),
                        exchange_order_id.clone(),
                        tracker(command),
                    );
                }
            }
        }
    }
}

pub fn maker_fill_from_tracked_private_event(
    runtime: &mut CrossArbRuntime,
    tracked: &TrackedCrossArbOrder,
    event: &PrivateEvent,
) -> Option<MakerFill> {
    let PrivateEventKind::Fill(fill) = &event.kind else {
        return None;
    };
    if let Err(error) = runtime.state.apply_fill_event(&tracked.bundle_id, fill) {
        runtime.state.risk_events.push(super::RiskEventReadModel {
            event_id: format!("fill-apply-{}", event.received_at.timestamp_millis()),
            canonical_symbol: Some(fill.canonical_symbol.clone()),
            exchange: Some(fill.exchange.clone()),
            reason: super::RejectReason::RouteUnhealthy,
            message: error.to_string(),
            created_at: event.received_at,
        });
        return None;
    }

    if tracked.leg != BundleLeg::Maker || fill.reduce_only.unwrap_or(false) {
        return None;
    }
    if let Some(bundle) = runtime.state.open_bundles.get_mut(&tracked.bundle_id) {
        bundle.status = SimulatedBundleStatus::Hedging;
        bundle.updated_at = fill.filled_at;
    }
    let _ = runtime.state.position_manager.mark_bundle_status(
        &tracked.bundle_id,
        BundleStatus::Hedging,
        fill.filled_at,
    );

    Some(MakerFill {
        bundle_id: tracked.bundle_id.clone(),
        mode: tracked.mode,
        canonical_symbol: fill.canonical_symbol.clone(),
        taker_exchange: tracked.taker_exchange.clone(),
        taker_exchange_symbol: tracked.taker_exchange_symbol.clone(),
        maker_side: tracked.maker_side,
        filled_quantity: fill.quantity,
        hedge_price: None,
        max_slippage_pct: tracked.max_slippage_pct,
        filled_at: fill.filled_at,
    })
}

pub fn execution_request_from_signal(
    state: &CrossArbRuntimeState,
    signal: &ArbSignal,
    opportunity: &Opportunity,
) -> Result<ExecutionRequest> {
    if signal.action != ArbSignalAction::Open {
        anyhow::bail!("signal {} is not an open signal", signal.signal_id);
    }
    if !opportunity.can_open {
        anyhow::bail!(
            "opportunity {} cannot open: {:?}",
            opportunity.opportunity_id,
            opportunity.reject_reasons
        );
    }
    let quantity = opportunity
        .maker_quantity
        .or_else(|| {
            quantity_from_notional(
                opportunity.executable_notional_usdt,
                opportunity.maker_price,
            )
        })
        .ok_or_else(|| {
            anyhow!(
                "opportunity {} has no executable quantity",
                opportunity.opportunity_id
            )
        })?;
    if quantity <= 0.0 || !quantity.is_finite() {
        anyhow::bail!(
            "opportunity {} has invalid quantity {quantity}",
            opportunity.opportunity_id
        );
    }

    let bundle_id = format!("bundle-{}", signal.signal_id);
    Ok(ExecutionRequest {
        request_id: format!("request-{}", signal.signal_id),
        mode: signal.mode,
        action: ExecutionAction::Open,
        bundle_id,
        canonical_symbol: opportunity.canonical_symbol.clone(),
        maker_exchange: opportunity.maker_exchange.clone(),
        taker_exchange: opportunity.taker_exchange.clone(),
        maker_exchange_symbol: exchange_symbol_for(
            &opportunity.maker_exchange,
            &opportunity.canonical_symbol,
        ),
        taker_exchange_symbol: exchange_symbol_for(
            &opportunity.taker_exchange,
            &opportunity.canonical_symbol,
        ),
        maker_side: to_execution_order_side(opportunity.maker_side),
        taker_side: to_execution_order_side(opportunity.taker_side),
        quantity,
        maker_price: Some(opportunity.maker_price),
        taker_price: opportunity.taker_vwap,
        max_slippage_pct: Some(state.config.execution.taker_ioc_slippage_limit_pct),
        generated_at: signal.generated_at,
    })
}

fn register_open_bundle(
    runtime: &mut CrossArbRuntime,
    signal: &ArbSignal,
    opportunity: &Opportunity,
    request: &ExecutionRequest,
) -> Result<()> {
    if runtime.state.open_bundles.len() >= runtime.state.config.risk.max_open_bundles {
        anyhow::bail!("max_open_bundles reached");
    }
    let now = signal.generated_at;
    let mut bundle = ArbitrageBundle::new(
        request.bundle_id.clone(),
        signal.mode,
        opportunity.canonical_symbol.clone(),
        opportunity.long_exchange.clone(),
        opportunity.short_exchange.clone(),
        opportunity.maker_exchange.clone(),
        opportunity.taker_exchange.clone(),
        opportunity.executable_notional_usdt,
        now,
    );
    bundle.status = BundleStatus::MakerPending;
    bundle.created_from_signal_id = Some(signal.signal_id.clone());
    runtime.state.register_bundle_position(
        &bundle,
        request.quantity,
        opportunity.maker_taker_net_edge,
        now,
    );
    runtime.state.open_bundles.insert(
        request.bundle_id.clone(),
        SimulatedBundleState {
            bundle_id: request.bundle_id.clone(),
            opportunity_id: opportunity.opportunity_id.clone(),
            status: SimulatedBundleStatus::MakerPending,
            route: opportunity.route(),
            target_notional_usdt: opportunity.executable_notional_usdt,
            opened_at: None,
            updated_at: now,
        },
    );
    if let Some(bundle) = runtime.state.open_bundles.get(&request.bundle_id) {
        runtime
            .storage
            .record(super::CrossArbStorageEvent::Bundle(bundle.clone()), now);
    }
    Ok(())
}

fn update_runtime_after_open_decision(runtime: &mut CrossArbRuntime, decision: &EngineDecision) {
    if let Some(reason) = &decision.blocked_reason {
        runtime.state.risk_events.push(super::RiskEventReadModel {
            event_id: format!(
                "execution-blocked-{}",
                decision.plan.created_at.timestamp_millis()
            ),
            canonical_symbol: decision
                .plan
                .commands
                .first()
                .map(|command| command.canonical_symbol.clone()),
            exchange: decision
                .plan
                .commands
                .first()
                .map(|command| command.exchange.clone()),
            reason: super::RejectReason::RouteUnhealthy,
            message: reason.clone(),
            created_at: decision.plan.created_at,
        });
        if decision.plan.requires_reconcile || decision.requires_reconcile {
            if let Some(bundle_id) = decision
                .plan
                .commands
                .first()
                .map(|command| command.bundle_id.clone())
            {
                if let Some(bundle) = runtime.state.open_bundles.get_mut(&bundle_id) {
                    bundle.status = SimulatedBundleStatus::OrphanLeg;
                    bundle.updated_at = decision.plan.created_at;
                }
                let _ = runtime.state.position_manager.mark_bundle_status(
                    &bundle_id,
                    BundleStatus::ReconcileRequired,
                    decision.plan.created_at,
                );
            }
        }
    }
}

fn tracked_order(command: &OrderCommand) -> TrackedCrossArbOrder {
    TrackedCrossArbOrder {
        bundle_id: command.bundle_id.clone(),
        leg: command_leg(command),
        exchange: command.exchange.clone(),
        exchange_symbol: command.exchange_symbol.clone(),
        maker_side: maker_side_from_command(command),
        taker_exchange: command.exchange.clone(),
        taker_exchange_symbol: command.exchange_symbol.clone(),
        max_slippage_pct: command.max_slippage_pct,
        mode: crate::market::RuntimeMode::Simulation,
        created_at: command.created_at,
    }
}

fn tracked_order_for_request(
    command: &OrderCommand,
    request: &ExecutionRequest,
) -> TrackedCrossArbOrder {
    let mut tracked = tracked_order(command);
    tracked.mode = request.mode;
    tracked.maker_side = request.maker_side;
    tracked.taker_exchange = request.taker_exchange.clone();
    tracked.taker_exchange_symbol = request.taker_exchange_symbol.clone();
    tracked.max_slippage_pct = request.max_slippage_pct;
    tracked
}

fn command_leg(command: &OrderCommand) -> BundleLeg {
    if command.post_only {
        BundleLeg::Maker
    } else if matches!(
        command.intent,
        crate::execution::OrderIntent::HedgeLongTaker
            | crate::execution::OrderIntent::HedgeShortTaker
    ) {
        BundleLeg::Hedge
    } else {
        BundleLeg::Taker
    }
}

fn maker_side_from_command(command: &OrderCommand) -> OrderSide {
    if command_leg(command) == BundleLeg::Maker {
        command.side
    } else {
        command.side.opposite()
    }
}

fn to_execution_order_side(side: super::state::OrderSide) -> OrderSide {
    match side {
        super::state::OrderSide::Buy => OrderSide::Buy,
        super::state::OrderSide::Sell => OrderSide::Sell,
    }
}

fn quantity_from_notional(notional: f64, price: f64) -> Option<f64> {
    (notional > 0.0 && price > 0.0 && notional.is_finite() && price.is_finite())
        .then_some(notional / price)
}

fn normalize_client_order_id(value: &str) -> String {
    value
        .strip_prefix("t-")
        .unwrap_or(value)
        .trim()
        .to_ascii_lowercase()
}

trait OrderSideExt {
    fn opposite(self) -> Self;
}

impl OrderSideExt for OrderSide {
    fn opposite(self) -> Self {
        match self {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }
}

pub fn maker_client_order_id(mode: crate::market::RuntimeMode, bundle_id: &str) -> String {
    deterministic_client_order_id(mode, bundle_id, BundleLeg::Maker, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{
        CancelAck, CancelCommand, ExchangeBalance, ExchangePosition, FillLiquidity, OrderAck,
        OrderCommandStatus, OrderQuery, OrderState, TimeInForce, TradingAdapter,
        TradingCapabilities,
    };
    use crate::market::{BookLevel, CanonicalSymbol, ExchangeSymbol, OrderBook5, RuntimeMode};
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct MockTradingAdapter {
        exchange: ExchangeId,
        place_calls: Arc<AtomicUsize>,
        cancel_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TradingAdapter for MockTradingAdapter {
        fn exchange(&self) -> ExchangeId {
            self.exchange.clone()
        }

        fn capabilities(&self) -> TradingCapabilities {
            TradingCapabilities::default()
        }

        async fn place_order(&self, command: OrderCommand) -> anyhow::Result<OrderAck> {
            self.place_calls.fetch_add(1, Ordering::SeqCst);
            Ok(OrderAck {
                exchange: command.exchange,
                client_order_id: command.client_order_id,
                exchange_order_id: Some("exchange-order-1".to_string()),
                accepted: true,
                status: OrderCommandStatus::Accepted,
                message: None,
                acknowledged_at: Utc::now(),
            })
        }

        async fn cancel_order(&self, command: CancelCommand) -> anyhow::Result<CancelAck> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CancelAck {
                exchange: command.exchange,
                client_order_id: command.client_order_id,
                exchange_order_id: command.exchange_order_id,
                accepted: true,
                status: OrderCommandStatus::Cancelled,
                message: None,
                acknowledged_at: Utc::now(),
            })
        }

        async fn get_order(&self, _query: OrderQuery) -> anyhow::Result<OrderState> {
            anyhow::bail!("not used")
        }

        async fn get_open_orders(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> anyhow::Result<Vec<OrderState>> {
            Ok(Vec::new())
        }

        async fn get_positions(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> anyhow::Result<Vec<ExchangePosition>> {
            Ok(Vec::new())
        }

        async fn get_balances(&self) -> anyhow::Result<Vec<ExchangeBalance>> {
            Ok(Vec::new())
        }
    }

    fn book(exchange: ExchangeId, bid: f64, ask: f64) -> OrderBook5 {
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol_for(&exchange, &CanonicalSymbol::new("BTC", "USDT")),
            vec![BookLevel::new(bid, 10.0)],
            vec![BookLevel::new(ask, 10.0)],
            Utc::now(),
            Utc::now(),
            Some(1),
            None,
        )
    }

    fn runtime_with_signal(now: DateTime<Utc>) -> (CrossArbRuntime, ArbSignal) {
        let mut config = super::super::CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.execution.dry_run = false;
        config.thresholds.min_open_maker_taker_net_edge = 0.001;
        let mut runtime = CrossArbRuntime::new(config, now);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let signals = runtime.on_market_snapshots(
            &symbol,
            &[
                super::super::MarketSnapshot::healthy(book(ExchangeId::Binance, 100.0, 101.0)),
                super::super::MarketSnapshot::healthy(book(ExchangeId::Okx, 105.0, 106.0)),
            ],
            now,
        );
        let signal = signals
            .into_iter()
            .find(|signal| signal.action == ArbSignalAction::Open)
            .expect("open signal");
        (runtime, signal)
    }

    #[tokio::test]
    async fn cross_arb_execution_should_submit_open_maker_order_and_index_it() {
        let now = Utc::now();
        let (mut runtime, signal) = runtime_with_signal(now);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut router = crate::execution::ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
        }));
        let mut coordinator =
            CrossArbExecutionCoordinator::new(crate::execution::ExecutionEngine::new(router));

        let decision = coordinator
            .execute_open_signal(&mut runtime, &signal)
            .await
            .unwrap()
            .expect("decision");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(decision.plan.commands.len(), 1);
        assert_eq!(
            decision.plan.commands[0].time_in_force,
            TimeInForce::PostOnly
        );
        assert!(runtime
            .state
            .open_bundles
            .contains_key(&decision.plan.commands[0].bundle_id));
        assert!(coordinator
            .order_index()
            .resolve_fill(&maker_fill_from_command(&decision.plan.commands[0]))
            .is_some());
    }

    #[tokio::test]
    async fn cross_arb_execution_should_turn_maker_fill_into_hedge_order() {
        let now = Utc::now();
        let (mut runtime, signal) = runtime_with_signal(now);
        let place_calls = Arc::new(AtomicUsize::new(0));
        let mut router = crate::execution::ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: place_calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: place_calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
        }));
        let mut coordinator =
            CrossArbExecutionCoordinator::new(crate::execution::ExecutionEngine::new(router));
        let open = coordinator
            .execute_open_signal(&mut runtime, &signal)
            .await
            .unwrap()
            .expect("open decision");

        let fill = maker_fill_from_command(&open.plan.commands[0]);
        let hedge = coordinator
            .hedge_candidate_from_private_event(
                &mut runtime,
                &PrivateEvent::fill(fill.clone(), fill.received_at),
            )
            .expect("hedge candidate");
        let hedge_decision = coordinator.execute_hedge_for_maker_fill(hedge).await;

        assert_eq!(place_calls.load(Ordering::SeqCst), 2);
        assert_eq!(hedge_decision.plan.commands.len(), 1);
        assert_ne!(
            hedge_decision.plan.commands[0].exchange,
            open.plan.commands[0].exchange
        );
        assert_eq!(
            hedge_decision.plan.commands[0].time_in_force,
            TimeInForce::Ioc
        );
    }

    #[tokio::test]
    async fn cross_arb_execution_should_cancel_expired_maker_order() {
        let now = Utc::now();
        let (mut runtime, signal) = runtime_with_signal(now);
        runtime.state.config.execution.maker_order_ttl_ms = 1;
        let place_calls = Arc::new(AtomicUsize::new(0));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let mut router = crate::execution::ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: place_calls.clone(),
            cancel_calls: cancel_calls.clone(),
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls,
            cancel_calls: cancel_calls.clone(),
        }));
        let mut coordinator =
            CrossArbExecutionCoordinator::new(crate::execution::ExecutionEngine::new(router));
        let open = coordinator
            .execute_open_signal(&mut runtime, &signal)
            .await
            .unwrap()
            .expect("open decision");

        let acks = coordinator
            .cancel_expired_maker_orders(&mut runtime, now + chrono::Duration::milliseconds(2))
            .await;

        assert_eq!(acks.len(), 1);
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(coordinator.pending_maker_count(), 0);
        assert_eq!(
            runtime
                .state
                .open_bundles
                .get(&open.plan.commands[0].bundle_id)
                .map(|bundle| bundle.status),
            Some(SimulatedBundleStatus::RiskStopped)
        );
    }

    fn maker_fill_from_command(command: &OrderCommand) -> FillEvent {
        FillEvent {
            exchange: command.exchange.clone(),
            canonical_symbol: command.canonical_symbol.clone(),
            exchange_symbol: command.exchange_symbol.clone(),
            trade_id: format!("trade-{}", command.client_order_id),
            client_order_id: Some(command.client_order_id.clone()),
            exchange_order_id: Some("exchange-order-1".to_string()),
            side: command.side,
            position_side: command.position_side,
            liquidity: FillLiquidity::Maker,
            price: command.price.unwrap_or(100.0),
            quantity: command.quantity,
            quote_quantity: command.price.unwrap_or(100.0) * command.quantity,
            fee: Some(0.01),
            fee_asset: Some("USDT".to_string()),
            fee_rate: Some(0.0001),
            realized_pnl: None,
            reduce_only: Some(false),
            filled_at: Utc::now(),
            received_at: Utc::now(),
        }
    }
}
