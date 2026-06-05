use crate::execution::{
    BundleLeg, ExecutionRouter, HedgePlanner, MakerFill, OrderAck, OrderCommand, OrderIntent,
    OrderSide, OrderType, PositionSide, TimeInForce,
};
use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol, RuntimeMode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionAction {
    Open,
    Close,
    EmergencyClose,
    Cancel,
    Noop,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRequest {
    pub request_id: String,
    pub mode: RuntimeMode,
    pub action: ExecutionAction,
    pub bundle_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub maker_exchange_symbol: ExchangeSymbol,
    pub taker_exchange_symbol: ExchangeSymbol,
    pub maker_side: OrderSide,
    pub taker_side: OrderSide,
    pub quantity: f64,
    pub maker_price: Option<f64>,
    pub taker_price: Option<f64>,
    pub max_slippage_pct: Option<f64>,
    pub generated_at: DateTime<Utc>,
    #[serde(default)]
    pub open_with_dual_taker: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub request_id: String,
    pub mode: RuntimeMode,
    pub commands: Vec<OrderCommand>,
    pub blocked_reason: Option<String>,
    pub requires_reconcile: bool,
    pub created_at: DateTime<Utc>,
}

impl ExecutionPlan {
    pub fn blocked(
        request: &ExecutionRequest,
        reason: impl Into<String>,
        requires_reconcile: bool,
    ) -> Self {
        Self {
            request_id: request.request_id.clone(),
            mode: request.mode,
            commands: Vec::new(),
            blocked_reason: Some(reason.into()),
            requires_reconcile,
            created_at: request.generated_at,
        }
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked_reason.is_some()
    }
}

impl ExecutionRequest {
    fn long_exchange(&self) -> ExchangeId {
        if self.maker_side == OrderSide::Buy {
            self.maker_exchange.clone()
        } else {
            self.taker_exchange.clone()
        }
    }

    fn short_exchange(&self) -> ExchangeId {
        if self.maker_side == OrderSide::Sell {
            self.maker_exchange.clone()
        } else {
            self.taker_exchange.clone()
        }
    }

    fn long_exchange_symbol(&self) -> ExchangeSymbol {
        if self.maker_side == OrderSide::Buy {
            self.maker_exchange_symbol.clone()
        } else {
            self.taker_exchange_symbol.clone()
        }
    }

    fn short_exchange_symbol(&self) -> ExchangeSymbol {
        if self.maker_side == OrderSide::Sell {
            self.maker_exchange_symbol.clone()
        } else {
            self.taker_exchange_symbol.clone()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EngineDecision {
    pub plan: ExecutionPlan,
    pub submitted_orders: Vec<OrderAck>,
    pub blocked_reason: Option<String>,
    pub requires_reconcile: bool,
}

impl EngineDecision {
    fn from_plan(plan: ExecutionPlan) -> Self {
        Self {
            blocked_reason: plan.blocked_reason.clone(),
            requires_reconcile: plan.requires_reconcile,
            plan,
            submitted_orders: Vec::new(),
        }
    }
}

pub struct ExecutionEngine {
    router: ExecutionRouter,
}

impl ExecutionEngine {
    pub fn new(router: ExecutionRouter) -> Self {
        Self { router }
    }

    pub fn router(&self) -> &ExecutionRouter {
        &self.router
    }

    pub fn plan_request(&self, request: &ExecutionRequest) -> ExecutionPlan {
        if matches!(request.action, ExecutionAction::Noop) {
            return ExecutionPlan {
                request_id: request.request_id.clone(),
                mode: request.mode,
                commands: Vec::new(),
                blocked_reason: None,
                requires_reconcile: false,
                created_at: request.generated_at,
            };
        }

        if request.quantity <= 0.0 || !request.quantity.is_finite() {
            return ExecutionPlan::blocked(request, "invalid execution quantity", false);
        }
        if request.mode.allows_live_orders()
            && matches!(request.action, ExecutionAction::Open)
            && request.open_with_dual_taker
        {
            if request
                .max_slippage_pct
                .filter(|value| value.is_finite() && *value > 0.0)
                .is_none()
            {
                return ExecutionPlan::blocked(
                    request,
                    "live taker-taker requires max slippage protection",
                    false,
                );
            }
            if self.long_taker_limit_price(request).is_none()
                || self.short_taker_limit_price(request).is_none()
            {
                return ExecutionPlan::blocked(
                    request,
                    "live taker-taker requires explicit IOC limit prices",
                    false,
                );
            }
        }
        if request.mode.allows_live_orders()
            && matches!(
                request.action,
                ExecutionAction::Close | ExecutionAction::EmergencyClose
            )
        {
            if request
                .max_slippage_pct
                .filter(|value| value.is_finite() && *value > 0.0)
                .is_none()
            {
                return ExecutionPlan::blocked(
                    request,
                    "live taker close requires max slippage protection",
                    false,
                );
            }
            if request.taker_price.is_none() {
                return ExecutionPlan::blocked(
                    request,
                    "live taker close requires explicit IOC limit price",
                    false,
                );
            }
        }

        let commands = match request.action {
            ExecutionAction::Open if request.open_with_dual_taker => {
                self.open_dual_taker_commands(request)
            }
            ExecutionAction::Open => self.open_maker_command(request).into_iter().collect(),
            ExecutionAction::Close => self
                .close_taker_command(request, false)
                .into_iter()
                .collect(),
            ExecutionAction::EmergencyClose => self
                .close_taker_command(request, true)
                .into_iter()
                .collect(),
            ExecutionAction::Cancel | ExecutionAction::Noop => Vec::new(),
        };

        ExecutionPlan {
            request_id: request.request_id.clone(),
            mode: request.mode,
            commands,
            blocked_reason: None,
            requires_reconcile: false,
            created_at: request.generated_at,
        }
    }

    pub async fn execute_request(&self, request: ExecutionRequest) -> EngineDecision {
        let mut decision = EngineDecision::from_plan(self.plan_request(&request));
        if decision.plan.is_blocked() || decision.plan.commands.is_empty() {
            return decision;
        }

        self.submit_plan(request.mode, &mut decision).await;
        decision
    }

    pub async fn submit_commands(&self, plan: ExecutionPlan) -> EngineDecision {
        let mut decision = EngineDecision::from_plan(plan);
        if decision.plan.is_blocked() || decision.plan.commands.is_empty() {
            return decision;
        }

        self.submit_plan(decision.plan.mode, &mut decision).await;
        decision
    }

    pub fn plan_hedge_for_maker_fill(&self, fill: &MakerFill) -> ExecutionPlan {
        if fill.filled_quantity <= 0.0 || !fill.filled_quantity.is_finite() {
            return ExecutionPlan {
                request_id: format!("hedge-{}", fill.bundle_id),
                mode: fill.mode,
                commands: Vec::new(),
                blocked_reason: Some("invalid maker fill quantity".to_string()),
                requires_reconcile: false,
                created_at: fill.filled_at,
            };
        }

        let command = HedgePlanner::hedge_for_maker_fill(fill);
        if fill.mode.allows_live_orders()
            && command.is_some()
            && (fill.hedge_price.is_none()
                || fill
                    .max_slippage_pct
                    .filter(|value| value.is_finite() && *value > 0.0)
                    .is_none())
        {
            return ExecutionPlan {
                request_id: format!("hedge-{}", fill.bundle_id),
                mode: fill.mode,
                commands: Vec::new(),
                blocked_reason: Some(
                    "live maker hedge requires explicit IOC limit price and max slippage"
                        .to_string(),
                ),
                requires_reconcile: false,
                created_at: fill.filled_at,
            };
        }
        ExecutionPlan {
            request_id: format!("hedge-{}", fill.bundle_id),
            mode: fill.mode,
            commands: command.into_iter().collect(),
            blocked_reason: None,
            requires_reconcile: false,
            created_at: fill.filled_at,
        }
    }

    pub async fn execute_hedge_for_maker_fill(&self, fill: MakerFill) -> EngineDecision {
        let mut decision = EngineDecision::from_plan(self.plan_hedge_for_maker_fill(&fill));
        if decision.plan.is_blocked() || decision.plan.commands.is_empty() {
            return decision;
        }

        self.submit_plan(fill.mode, &mut decision).await;
        decision
    }

    async fn submit_plan(&self, mode: RuntimeMode, decision: &mut EngineDecision) {
        if !mode.allows_live_orders() || self.router.dry_run() {
            return;
        }
        let missing = self.router.missing_adapters_for(
            decision
                .plan
                .commands
                .iter()
                .map(|command| &command.exchange),
        );
        if !missing.is_empty() {
            let reason = format!(
                "missing trading adapter for exchange(s): {}",
                missing
                    .iter()
                    .map(|exchange| exchange.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            decision.plan.blocked_reason = Some(reason.clone());
            decision.blocked_reason = Some(reason);
            return;
        }

        for command in decision.plan.commands.clone() {
            log_live_order_submission(mode, &command);
            match self.router.route_order(command.clone()).await {
                Ok(ack) => {
                    log_live_order_ack(mode, &command, &ack);
                    decision.submitted_orders.push(ack);
                }
                Err(error) => {
                    log_live_order_rejection(mode, &command, &error.to_string());
                    decision.plan.blocked_reason = Some(error.to_string());
                    decision.blocked_reason = Some(error.to_string());
                    decision.plan.requires_reconcile = true;
                    decision.requires_reconcile = true;
                    break;
                }
            }
        }
    }

    fn open_maker_command(&self, request: &ExecutionRequest) -> Option<OrderCommand> {
        let (intent, position_side) = match request.maker_side {
            OrderSide::Buy => (OrderIntent::OpenLongMaker, PositionSide::Long),
            OrderSide::Sell => (OrderIntent::OpenShortMaker, PositionSide::Short),
        };

        Some(OrderCommand::new(
            request.mode,
            request.bundle_id.clone(),
            BundleLeg::Maker,
            1,
            request.maker_exchange.clone(),
            request.canonical_symbol.clone(),
            request.maker_exchange_symbol.clone(),
            intent,
            request.maker_side,
            position_side,
            OrderType::Limit,
            request.quantity,
            request.maker_price,
            TimeInForce::PostOnly,
            true,
            false,
            request.max_slippage_pct,
            request.generated_at,
        ))
    }

    fn open_dual_taker_commands(&self, request: &ExecutionRequest) -> Vec<OrderCommand> {
        vec![
            OrderCommand::new(
                request.mode,
                request.bundle_id.clone(),
                BundleLeg::Long,
                1,
                request.long_exchange(),
                request.canonical_symbol.clone(),
                request.long_exchange_symbol(),
                OrderIntent::HedgeLongTaker,
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                request.quantity,
                self.long_taker_limit_price(request),
                TimeInForce::Ioc,
                false,
                false,
                request.max_slippage_pct,
                request.generated_at,
            ),
            OrderCommand::new(
                request.mode,
                request.bundle_id.clone(),
                BundleLeg::Short,
                1,
                request.short_exchange(),
                request.canonical_symbol.clone(),
                request.short_exchange_symbol(),
                OrderIntent::HedgeShortTaker,
                OrderSide::Sell,
                PositionSide::Short,
                OrderType::Limit,
                request.quantity,
                self.short_taker_limit_price(request),
                TimeInForce::Ioc,
                false,
                false,
                request.max_slippage_pct,
                request.generated_at,
            ),
        ]
    }

    fn long_taker_limit_price(&self, request: &ExecutionRequest) -> Option<f64> {
        if request.maker_side == OrderSide::Buy {
            request.maker_price
        } else {
            request.taker_price
        }
    }

    fn short_taker_limit_price(&self, request: &ExecutionRequest) -> Option<f64> {
        if request.maker_side == OrderSide::Sell {
            request.maker_price
        } else {
            request.taker_price
        }
    }

    fn close_taker_command(
        &self,
        request: &ExecutionRequest,
        emergency: bool,
    ) -> Option<OrderCommand> {
        let (intent, position_side, leg) = match (request.taker_side, emergency) {
            (OrderSide::Sell, false) => (
                OrderIntent::CloseLongTaker,
                PositionSide::Long,
                BundleLeg::CloseLong,
            ),
            (OrderSide::Buy, false) => (
                OrderIntent::CloseShortTaker,
                PositionSide::Short,
                BundleLeg::CloseShort,
            ),
            (OrderSide::Sell, true) => (
                OrderIntent::EmergencyCloseLongTaker,
                PositionSide::Long,
                BundleLeg::EmergencyCloseLong,
            ),
            (OrderSide::Buy, true) => (
                OrderIntent::EmergencyCloseShortTaker,
                PositionSide::Short,
                BundleLeg::EmergencyCloseShort,
            ),
        };

        Some(OrderCommand::new(
            request.mode,
            request.bundle_id.clone(),
            leg,
            1,
            request.taker_exchange.clone(),
            request.canonical_symbol.clone(),
            request.taker_exchange_symbol.clone(),
            intent,
            request.taker_side,
            position_side,
            if request.mode.allows_live_orders() {
                OrderType::Limit
            } else {
                OrderType::Market
            },
            request.quantity,
            request.taker_price,
            TimeInForce::Ioc,
            false,
            true,
            request.max_slippage_pct,
            request.generated_at,
        ))
    }
}

fn log_live_order_submission(mode: RuntimeMode, command: &OrderCommand) {
    if !mode.allows_live_orders() {
        return;
    }
    log::warn!(
        "cross-arb live submitted order command_id={} bundle_id={} exchange={} symbol={} side={:?} position_side={:?} intent={:?} order_type={:?} tif={:?} quantity={} price={:?} max_slippage_pct={:?} reduce_only={} post_only={} client_order_id={}",
        command.command_id,
        command.bundle_id,
        command.exchange,
        command.canonical_symbol,
        command.side,
        command.position_side,
        command.intent,
        command.order_type,
        command.time_in_force,
        command.quantity,
        command.price,
        command.max_slippage_pct,
        command.reduce_only,
        command.post_only,
        command.client_order_id
    );
    if is_hedge_intent(command.intent) {
        log::warn!(
            "cross-arb live hedge submitted bundle_id={} exchange={} symbol={} intent={:?} quantity={} price={:?}",
            command.bundle_id,
            command.exchange,
            command.canonical_symbol,
            command.intent,
            command.quantity,
            command.price
        );
    }
}

fn log_live_order_ack(mode: RuntimeMode, command: &OrderCommand, ack: &OrderAck) {
    if !mode.allows_live_orders() {
        return;
    }
    log::warn!(
        "cross-arb live order update command_id={} bundle_id={} exchange={} symbol={} client_order_id={} exchange_order_id={:?} accepted={} status={:?} message={:?}",
        command.command_id,
        command.bundle_id,
        ack.exchange,
        command.canonical_symbol,
        ack.client_order_id,
        ack.exchange_order_id,
        ack.accepted,
        ack.status,
        ack.message
    );
}

fn log_live_order_rejection(mode: RuntimeMode, command: &OrderCommand, reason: &str) {
    if !mode.allows_live_orders() {
        return;
    }
    log::error!(
        "cross-arb live order rejected command_id={} bundle_id={} exchange={} symbol={} intent={:?} reason={}",
        command.command_id,
        command.bundle_id,
        command.exchange,
        command.canonical_symbol,
        command.intent,
        reason
    );
}

fn is_hedge_intent(intent: OrderIntent) -> bool {
    matches!(
        intent,
        OrderIntent::HedgeLongTaker
            | OrderIntent::HedgeShortTaker
            | OrderIntent::EmergencyCloseLongTaker
            | OrderIntent::EmergencyCloseShortTaker
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{
        CancelAck, CancelCommand, ExchangeBalance, ExchangePosition, OrderQuery, OrderState,
        TradingAdapter, TradingCapabilities,
    };
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct MockTradingAdapter {
        exchange: ExchangeId,
        place_calls: Arc<AtomicUsize>,
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
                exchange_order_id: Some("mock-order-1".to_string()),
                accepted: true,
                status: crate::execution::OrderCommandStatus::Accepted,
                message: None,
                acknowledged_at: Utc::now(),
            })
        }

        async fn cancel_order(&self, command: CancelCommand) -> anyhow::Result<CancelAck> {
            Ok(CancelAck {
                exchange: command.exchange,
                client_order_id: command.client_order_id,
                exchange_order_id: command.exchange_order_id,
                accepted: true,
                status: crate::execution::OrderCommandStatus::Cancelled,
                message: None,
                acknowledged_at: Utc::now(),
            })
        }

        async fn get_order(&self, _query: OrderQuery) -> anyhow::Result<OrderState> {
            unimplemented!("mock get_order is not used by execution engine tests")
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

    fn request(mode: RuntimeMode) -> ExecutionRequest {
        ExecutionRequest {
            request_id: "request-1".to_string(),
            mode,
            action: ExecutionAction::Open,
            bundle_id: "bundle-1".to_string(),
            canonical_symbol: CanonicalSymbol::new("btc", "usdt"),
            maker_exchange: ExchangeId::Binance,
            taker_exchange: ExchangeId::Okx,
            maker_exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            taker_exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            maker_side: OrderSide::Buy,
            taker_side: OrderSide::Sell,
            quantity: 0.1,
            maker_price: Some(100.0),
            taker_price: None,
            max_slippage_pct: Some(0.001),
            generated_at: Utc::now(),
            open_with_dual_taker: false,
        }
    }

    fn maker_fill(mode: RuntimeMode) -> MakerFill {
        MakerFill {
            bundle_id: "bundle-1".to_string(),
            mode,
            canonical_symbol: CanonicalSymbol::new("btc", "usdt"),
            taker_exchange: ExchangeId::Okx,
            taker_exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            maker_side: OrderSide::Sell,
            filled_quantity: 0.1,
            hedge_price: Some(101.0),
            max_slippage_pct: Some(0.001),
            reduce_only: false,
            filled_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn engine_should_not_send_orders_in_simulation_or_shadow() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut router = ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: calls.clone(),
        }));
        let engine = ExecutionEngine::new(router);

        let simulation = engine
            .execute_request(request(RuntimeMode::Simulation))
            .await;
        let shadow = engine.execute_request(request(RuntimeMode::Shadow)).await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(simulation.plan.commands.len(), 1);
        assert_eq!(shadow.plan.commands.len(), 1);
        assert!(simulation.submitted_orders.is_empty());
        assert!(shadow.submitted_orders.is_empty());
    }

    #[tokio::test]
    async fn engine_should_not_send_orders_in_live_dry_run() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut router = ExecutionRouter::new(true);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: calls.clone(),
        }));
        let engine = ExecutionEngine::new(router);

        let decision = engine
            .execute_request(request(RuntimeMode::LiveSmall))
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(decision.plan.commands.len(), 1);
        assert!(decision.submitted_orders.is_empty());
    }

    #[tokio::test]
    async fn engine_should_send_orders_in_live_non_dry_run() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut router = ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: calls.clone(),
        }));
        let engine = ExecutionEngine::new(router);

        let decision = engine
            .execute_request(request(RuntimeMode::LiveSmall))
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(decision.submitted_orders.len(), 1);
        assert!(decision.blocked_reason.is_none());
    }

    #[tokio::test]
    async fn engine_should_submit_dual_taker_open_when_requested() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut router = ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: calls.clone(),
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: calls.clone(),
        }));
        let engine = ExecutionEngine::new(router);
        let mut request = request(RuntimeMode::LiveSmall);
        request.open_with_dual_taker = true;
        request.taker_price = Some(101.0);

        let decision = engine.execute_request(request).await;

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(decision.submitted_orders.len(), 2);
        assert!(decision.blocked_reason.is_none());
        let long = &decision.plan.commands[0];
        let short = &decision.plan.commands[1];
        assert_eq!(long.exchange, ExchangeId::Binance);
        assert_eq!(long.side, OrderSide::Buy);
        assert_eq!(long.position_side, PositionSide::Long);
        assert_eq!(long.order_type, OrderType::Limit);
        assert_eq!(long.price, Some(100.0));
        assert_eq!(long.time_in_force, TimeInForce::Ioc);
        assert!(!long.post_only);
        assert!(!long.reduce_only);
        assert_eq!(short.exchange, ExchangeId::Okx);
        assert_eq!(short.side, OrderSide::Sell);
        assert_eq!(short.position_side, PositionSide::Short);
        assert_eq!(short.order_type, OrderType::Limit);
        assert_eq!(short.price, Some(101.0));
        assert_eq!(short.time_in_force, TimeInForce::Ioc);
        assert!(!short.post_only);
        assert!(!short.reduce_only);
    }

    #[tokio::test]
    async fn engine_should_block_live_dual_taker_without_ioc_limit_prices() {
        let engine = ExecutionEngine::new(ExecutionRouter::new(false));
        let mut request = request(RuntimeMode::LiveSmall);
        request.open_with_dual_taker = true;
        request.taker_price = None;

        let decision = engine.execute_request(request).await;

        assert_eq!(
            decision.blocked_reason.as_deref(),
            Some("live taker-taker requires explicit IOC limit prices")
        );
    }

    #[tokio::test]
    async fn engine_should_block_when_live_route_is_missing() {
        let engine = ExecutionEngine::new(ExecutionRouter::new(false));

        let decision = engine
            .execute_request(request(RuntimeMode::LiveSmall))
            .await;

        assert!(decision.submitted_orders.is_empty());
        assert!(decision
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("missing trading adapter"));
    }

    #[test]
    fn engine_should_plan_hedge_for_maker_fill() {
        let engine = ExecutionEngine::new(ExecutionRouter::new(true));

        let plan = engine.plan_hedge_for_maker_fill(&maker_fill(RuntimeMode::LiveSmall));

        assert_eq!(plan.commands.len(), 1);
        let command = &plan.commands[0];
        assert_eq!(command.exchange, ExchangeId::Okx);
        assert_eq!(command.intent, OrderIntent::HedgeLongTaker);
        assert_eq!(command.side, OrderSide::Buy);
        assert_eq!(command.time_in_force, TimeInForce::Ioc);
        assert_eq!(command.quantity, 0.1);
    }

    #[tokio::test]
    async fn engine_should_submit_hedge_for_maker_fill_in_live_non_dry_run() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut router = ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: calls.clone(),
        }));
        let engine = ExecutionEngine::new(router);

        let decision = engine
            .execute_hedge_for_maker_fill(maker_fill(RuntimeMode::LiveSmall))
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(decision.submitted_orders.len(), 1);
        assert!(decision.blocked_reason.is_none());
    }

    #[tokio::test]
    async fn engine_should_block_hedge_when_taker_adapter_is_missing() {
        let engine = ExecutionEngine::new(ExecutionRouter::new(false));

        let decision = engine
            .execute_hedge_for_maker_fill(maker_fill(RuntimeMode::LiveSmall))
            .await;

        assert!(decision.submitted_orders.is_empty());
        assert!(decision
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("missing trading adapter"));
    }
}
