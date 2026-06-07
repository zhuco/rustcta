#![allow(clippy::items_after_test_module)]

use crate::execution::{
    AmendOrderAck, AmendOrderCommand, BatchPlaceAck, BatchPlaceCommand, BundleLegSubmitResult,
    BundleSubmissionPath, BundleSubmitCommand, BundleSubmitReport, CancelAck, CancelAllAck,
    CancelAllCommand, CancelBatchAck, CancelBatchCommand, CancelCommand, ClosePositionAck,
    ClosePositionCommand, CountdownCancelAllAck, CountdownCancelAllCommand, LeverageAck,
    LeverageCommand, OrderAck, OrderCommand, PositionModeAck, PositionModeCommand, TradingAdapter,
};
use crate::market::ExchangeId;
use anyhow::{anyhow, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
pub struct ExecutionRouter {
    adapters: HashMap<ExchangeId, Arc<dyn TradingAdapter>>,
    dry_run: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{
        BundleExecutionStatus, BundleLeg, BundleOrderLeg, BundleSubmissionPath,
        BundleSubmitCommand, CountdownCancelAllCommand, ExchangeBalance, ExchangePosition,
        OneSidedExposureKind, OrderCommandStatus, OrderIntent, OrderQuery, OrderSide, OrderState,
        OrderType, PositionMode, PositionSide, TimeInForce, TradingCapabilities,
    };
    use crate::market::{CanonicalSymbol, ExchangeSymbol, RuntimeMode};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::{timeout, Duration};

    struct MockTradingAdapter {
        exchange: ExchangeId,
        leverage_calls: AtomicUsize,
    }

    #[async_trait]
    impl TradingAdapter for MockTradingAdapter {
        fn exchange(&self) -> ExchangeId {
            self.exchange.clone()
        }

        fn capabilities(&self) -> TradingCapabilities {
            TradingCapabilities::default()
        }

        async fn place_order(&self, command: OrderCommand) -> Result<OrderAck> {
            Ok(OrderAck {
                exchange: command.exchange,
                client_order_id: command.client_order_id,
                exchange_order_id: Some("mock-order".to_string()),
                accepted: true,
                status: OrderCommandStatus::Accepted,
                message: None,
                acknowledged_at: Utc::now(),
            })
        }

        async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck> {
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

        async fn get_order(&self, _query: OrderQuery) -> Result<OrderState> {
            anyhow::bail!("not used")
        }

        async fn get_open_orders(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<OrderState>> {
            Ok(Vec::new())
        }

        async fn get_positions(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<ExchangePosition>> {
            Ok(Vec::new())
        }

        async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
            Ok(Vec::new())
        }

        async fn set_leverage(&self, command: LeverageCommand) -> Result<LeverageAck> {
            self.leverage_calls.fetch_add(1, Ordering::SeqCst);
            Ok(LeverageAck {
                exchange: command.exchange,
                canonical_symbol: command.canonical_symbol,
                exchange_symbol: command.exchange_symbol,
                leverage: command.leverage,
                accepted: true,
                message: None,
                acknowledged_at: Utc::now(),
            })
        }
    }

    struct ConcurrentProbeAdapter {
        exchange: ExchangeId,
        entered: Arc<AtomicUsize>,
        notify: Arc<Notify>,
    }

    #[async_trait]
    impl TradingAdapter for ConcurrentProbeAdapter {
        fn exchange(&self) -> ExchangeId {
            self.exchange.clone()
        }

        fn capabilities(&self) -> TradingCapabilities {
            TradingCapabilities::default()
        }

        async fn place_order(&self, command: OrderCommand) -> Result<OrderAck> {
            let entered = self.entered.fetch_add(1, Ordering::SeqCst) + 1;
            if entered >= 2 {
                self.notify.notify_waiters();
            } else {
                timeout(Duration::from_millis(250), self.notify.notified())
                    .await
                    .expect("second bundle leg was not submitted before first ack");
            }
            Ok(accepted_ack(command, OrderCommandStatus::Accepted))
        }

        async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck> {
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

        async fn get_order(&self, _query: OrderQuery) -> Result<OrderState> {
            anyhow::bail!("not used")
        }

        async fn get_open_orders(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<OrderState>> {
            Ok(Vec::new())
        }

        async fn get_positions(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<ExchangePosition>> {
            Ok(Vec::new())
        }

        async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
            Ok(Vec::new())
        }
    }

    struct StaticAckAdapter {
        exchange: ExchangeId,
        status: OrderCommandStatus,
        accepted: bool,
    }

    #[async_trait]
    impl TradingAdapter for StaticAckAdapter {
        fn exchange(&self) -> ExchangeId {
            self.exchange.clone()
        }

        fn capabilities(&self) -> TradingCapabilities {
            TradingCapabilities::default()
        }

        async fn place_order(&self, command: OrderCommand) -> Result<OrderAck> {
            let mut ack = accepted_ack(command, self.status);
            ack.accepted = self.accepted;
            if !self.accepted {
                ack.message = Some("mock reject".to_string());
            }
            Ok(ack)
        }

        async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck> {
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

        async fn get_order(&self, _query: OrderQuery) -> Result<OrderState> {
            anyhow::bail!("not used")
        }

        async fn get_open_orders(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<OrderState>> {
            Ok(Vec::new())
        }

        async fn get_positions(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<ExchangePosition>> {
            Ok(Vec::new())
        }

        async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
            Ok(Vec::new())
        }
    }

    struct BatchProbeAdapter {
        exchange: ExchangeId,
        batch_calls: Arc<AtomicUsize>,
        single_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TradingAdapter for BatchProbeAdapter {
        fn exchange(&self) -> ExchangeId {
            self.exchange.clone()
        }

        fn capabilities(&self) -> TradingCapabilities {
            TradingCapabilities {
                supports_batch_place_orders: true,
                ..TradingCapabilities::default()
            }
        }

        async fn place_order(&self, command: OrderCommand) -> Result<OrderAck> {
            self.single_calls.fetch_add(1, Ordering::SeqCst);
            Ok(accepted_ack(command, OrderCommandStatus::Accepted))
        }

        async fn place_batch_orders(&self, command: BatchPlaceCommand) -> Result<BatchPlaceAck> {
            self.batch_calls.fetch_add(1, Ordering::SeqCst);
            let order_acks = command
                .orders
                .iter()
                .cloned()
                .map(|order| accepted_ack(order, OrderCommandStatus::Accepted))
                .collect::<Vec<_>>();
            Ok(BatchPlaceAck {
                exchange: command.exchange,
                accepted: true,
                placed_orders: order_acks.len(),
                order_acks,
                failed_orders: Vec::new(),
                message: None,
                acknowledged_at: Utc::now(),
            })
        }

        async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck> {
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

        async fn get_order(&self, _query: OrderQuery) -> Result<OrderState> {
            anyhow::bail!("not used")
        }

        async fn get_open_orders(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<OrderState>> {
            Ok(Vec::new())
        }

        async fn get_positions(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<ExchangePosition>> {
            Ok(Vec::new())
        }

        async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
            Ok(Vec::new())
        }
    }

    fn accepted_ack(command: OrderCommand, status: OrderCommandStatus) -> OrderAck {
        OrderAck {
            exchange: command.exchange,
            client_order_id: command.client_order_id,
            exchange_order_id: Some("mock-order".to_string()),
            accepted: !matches!(
                status,
                OrderCommandStatus::Rejected | OrderCommandStatus::Failed
            ),
            status,
            message: None,
            acknowledged_at: Utc::now(),
        }
    }

    fn bundle_order(
        exchange: ExchangeId,
        leg: BundleLeg,
        side: OrderSide,
        position_side: PositionSide,
        price: f64,
    ) -> OrderCommand {
        let intent = match side {
            OrderSide::Buy => OrderIntent::HedgeLongTaker,
            OrderSide::Sell => OrderIntent::HedgeShortTaker,
        };
        OrderCommand::new(
            RuntimeMode::LiveSmall,
            "bundle-1",
            leg,
            1,
            exchange.clone(),
            CanonicalSymbol::new("btc", "usdt"),
            ExchangeSymbol::new(exchange, "BTCUSDT"),
            intent,
            side,
            position_side,
            OrderType::Limit,
            0.1,
            Some(price),
            TimeInForce::Ioc,
            false,
            false,
            Some(0.001),
            Utc::now(),
        )
    }

    fn bundle_command(orders: [OrderCommand; 2]) -> BundleSubmitCommand {
        BundleSubmitCommand::new(
            RuntimeMode::LiveSmall,
            "bundle-1",
            "opportunity-1",
            "idem-1",
            [
                BundleOrderLeg::new(BundleLeg::Long, orders[0].clone()),
                BundleOrderLeg::new(BundleLeg::Short, orders[1].clone()),
            ],
            Utc::now(),
        )
    }

    #[tokio::test]
    async fn router_should_dry_run_leverage_without_adapter_side_effects() {
        let adapter = Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            leverage_calls: AtomicUsize::new(0),
        });
        let mut router = ExecutionRouter::new(true);
        router.register_adapter(adapter.clone());

        let ack = router
            .route_set_leverage(LeverageCommand {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                leverage: 2,
                requested_at: Utc::now(),
            })
            .await
            .expect("dry-run leverage ack");

        assert!(!ack.accepted);
        assert_eq!(adapter.leverage_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn router_should_dry_run_countdown_cancel_without_adapter_side_effects() {
        let adapter = Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Gate,
            leverage_calls: AtomicUsize::new(0),
        });
        let mut router = ExecutionRouter::new(true);
        router.register_adapter(adapter.clone());

        let ack = router
            .route_set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Gate,
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                30,
                Utc::now(),
            ))
            .await
            .expect("dry-run countdown cancel-all ack");

        assert!(!ack.accepted);
        assert_eq!(ack.timeout_secs, 30);
    }

    #[tokio::test]
    async fn router_should_dry_run_batch_place_without_adapter_side_effects() {
        let adapter = Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            leverage_calls: AtomicUsize::new(0),
        });
        let mut router = ExecutionRouter::new(true);
        router.register_adapter(adapter);

        let order = OrderCommand::new(
            RuntimeMode::LiveSmall,
            "bundle-1",
            BundleLeg::Maker,
            1,
            ExchangeId::Binance,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            OrderIntent::OpenLongMaker,
            OrderSide::Buy,
            PositionSide::Long,
            OrderType::Limit,
            0.001,
            Some(65_000.0),
            TimeInForce::PostOnly,
            true,
            false,
            None,
            Utc::now(),
        );
        let ack = router
            .route_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Binance,
                [order],
                Utc::now(),
            ))
            .await
            .expect("dry-run batch-place ack");

        assert!(!ack.accepted);
        assert_eq!(ack.placed_orders, 0);
        assert_eq!(ack.order_acks.len(), 1);
    }

    #[tokio::test]
    async fn router_should_submit_cross_exchange_bundle_concurrently() {
        let entered = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let mut router = ExecutionRouter::new(false);
        router.register_adapter(Arc::new(ConcurrentProbeAdapter {
            exchange: ExchangeId::Binance,
            entered: entered.clone(),
            notify: notify.clone(),
        }));
        router.register_adapter(Arc::new(ConcurrentProbeAdapter {
            exchange: ExchangeId::Okx,
            entered: entered.clone(),
            notify,
        }));

        let report = router
            .submit_bundle_concurrent(bundle_command([
                bundle_order(
                    ExchangeId::Binance,
                    BundleLeg::Long,
                    OrderSide::Buy,
                    PositionSide::Long,
                    100.0,
                ),
                bundle_order(
                    ExchangeId::Okx,
                    BundleLeg::Short,
                    OrderSide::Sell,
                    PositionSide::Short,
                    101.0,
                ),
            ]))
            .await
            .expect("bundle submit");

        assert_eq!(entered.load(Ordering::SeqCst), 2);
        assert_eq!(report.submission_path, BundleSubmissionPath::Concurrent);
        assert_eq!(report.status, BundleExecutionStatus::Accepted);
        assert!(!report.requires_reconcile);
    }

    #[tokio::test]
    async fn router_should_prefer_batch_for_same_exchange_supported_adapter() {
        let batch_calls = Arc::new(AtomicUsize::new(0));
        let single_calls = Arc::new(AtomicUsize::new(0));
        let mut router = ExecutionRouter::new(false);
        router.register_adapter(Arc::new(BatchProbeAdapter {
            exchange: ExchangeId::Binance,
            batch_calls: batch_calls.clone(),
            single_calls: single_calls.clone(),
        }));

        let report = router
            .submit_bundle_concurrent(bundle_command([
                bundle_order(
                    ExchangeId::Binance,
                    BundleLeg::Long,
                    OrderSide::Buy,
                    PositionSide::Long,
                    100.0,
                ),
                bundle_order(
                    ExchangeId::Binance,
                    BundleLeg::Short,
                    OrderSide::Sell,
                    PositionSide::Short,
                    101.0,
                ),
            ]))
            .await
            .expect("bundle submit");

        assert_eq!(batch_calls.load(Ordering::SeqCst), 1);
        assert_eq!(single_calls.load(Ordering::SeqCst), 0);
        assert_eq!(report.submission_path, BundleSubmissionPath::Batch);
        assert_eq!(report.status, BundleExecutionStatus::Accepted);
    }

    #[tokio::test]
    async fn router_should_mark_one_leg_reject_as_one_sided_exposure() {
        let mut router = ExecutionRouter::new(false);
        router.register_adapter(Arc::new(StaticAckAdapter {
            exchange: ExchangeId::Binance,
            status: OrderCommandStatus::Filled,
            accepted: true,
        }));
        router.register_adapter(Arc::new(StaticAckAdapter {
            exchange: ExchangeId::Okx,
            status: OrderCommandStatus::Rejected,
            accepted: false,
        }));

        let report = router
            .submit_bundle_concurrent(bundle_command([
                bundle_order(
                    ExchangeId::Binance,
                    BundleLeg::Long,
                    OrderSide::Buy,
                    PositionSide::Long,
                    100.0,
                ),
                bundle_order(
                    ExchangeId::Okx,
                    BundleLeg::Short,
                    OrderSide::Sell,
                    PositionSide::Short,
                    101.0,
                ),
            ]))
            .await
            .expect("bundle submit");

        assert_eq!(report.status, BundleExecutionStatus::OneSidedExposure);
        assert!(report.requires_reconcile);
        let exposure = report.one_sided_exposure.expect("exposure");
        assert_eq!(exposure.kind, OneSidedExposureKind::LongBase);
        assert!(exposure
            .recovery_modes
            .contains(&crate::execution::BundleRecoveryMode::NoLoss));
        assert!(!exposure
            .recovery_modes
            .contains(&crate::execution::BundleRecoveryMode::EmergencyRisk));
    }

    #[tokio::test]
    async fn router_should_mark_partial_fill_as_explicit_bundle_state() {
        let mut router = ExecutionRouter::new(false);
        router.register_adapter(Arc::new(StaticAckAdapter {
            exchange: ExchangeId::Binance,
            status: OrderCommandStatus::PartiallyFilled,
            accepted: true,
        }));
        router.register_adapter(Arc::new(StaticAckAdapter {
            exchange: ExchangeId::Okx,
            status: OrderCommandStatus::Accepted,
            accepted: true,
        }));

        let report = router
            .submit_bundle_concurrent(bundle_command([
                bundle_order(
                    ExchangeId::Binance,
                    BundleLeg::Long,
                    OrderSide::Buy,
                    PositionSide::Long,
                    100.0,
                ),
                bundle_order(
                    ExchangeId::Okx,
                    BundleLeg::Short,
                    OrderSide::Sell,
                    PositionSide::Short,
                    101.0,
                ),
            ]))
            .await
            .expect("bundle submit");

        assert_eq!(report.status, BundleExecutionStatus::OneSidedExposure);
        assert!(report.requires_reconcile);
        assert!(report.one_sided_exposure.is_some());
    }

    #[tokio::test]
    async fn router_should_block_live_taker_bundle_without_max_slippage() {
        let mut unsafe_order = bundle_order(
            ExchangeId::Binance,
            BundleLeg::Long,
            OrderSide::Buy,
            PositionSide::Long,
            100.0,
        );
        unsafe_order.max_slippage_pct = None;

        let error = ExecutionRouter::new(false)
            .submit_bundle_concurrent(bundle_command([
                unsafe_order,
                bundle_order(
                    ExchangeId::Okx,
                    BundleLeg::Short,
                    OrderSide::Sell,
                    PositionSide::Short,
                    101.0,
                ),
            ]))
            .await
            .expect_err("unsafe live taker must be rejected");

        assert!(error.to_string().contains("max slippage protection"));
    }

    #[test]
    fn position_mode_should_identify_hedge_mode() {
        assert!(PositionMode::Hedge.is_hedge());
        assert!(!PositionMode::OneWay.is_hedge());
    }
}

impl ExecutionRouter {
    pub fn new(dry_run: bool) -> Self {
        Self {
            adapters: HashMap::new(),
            dry_run,
        }
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn set_dry_run(&mut self, dry_run: bool) {
        self.dry_run = dry_run;
    }

    pub fn register_adapter(&mut self, adapter: Arc<dyn TradingAdapter>) {
        self.adapters.insert(adapter.exchange(), adapter);
    }

    pub fn has_adapter(&self, exchange: &ExchangeId) -> bool {
        self.adapters.contains_key(exchange)
    }

    pub fn missing_adapters_for<'a>(
        &self,
        exchanges: impl IntoIterator<Item = &'a ExchangeId>,
    ) -> Vec<ExchangeId> {
        exchanges
            .into_iter()
            .filter(|exchange| !self.has_adapter(exchange))
            .cloned()
            .collect()
    }

    pub async fn route_order(&self, command: OrderCommand) -> Result<OrderAck> {
        if self.dry_run {
            return Ok(OrderAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.place_order(command).await
    }

    pub async fn route_batch_orders(&self, command: BatchPlaceCommand) -> Result<BatchPlaceAck> {
        if self.dry_run {
            return Ok(BatchPlaceAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.place_batch_orders(command).await
    }

    pub async fn submit_bundle_concurrent(
        &self,
        command: BundleSubmitCommand,
    ) -> Result<BundleSubmitReport> {
        command.validate()?;

        if self.dry_run {
            let results = command
                .legs
                .iter()
                .cloned()
                .map(|leg| {
                    let ack = OrderAck::dry_run(&leg.command, Utc::now());
                    BundleLegSubmitResult::acked(leg, ack)
                })
                .collect();
            return Ok(BundleSubmitReport::from_results(
                &command,
                BundleSubmissionPath::DryRun,
                results,
                Utc::now(),
            ));
        }

        if let Some(exchange) = command.same_exchange() {
            if let Some(adapter) = self.adapters.get(&exchange) {
                if adapter.capabilities().supports_batch_place_orders {
                    return self.submit_bundle_batch(command, exchange).await;
                }
            }
        }

        let [first, second] = command.legs.clone().try_into().map_err(|legs: Vec<_>| {
            anyhow!("expected exactly two bundle legs, got {}", legs.len())
        })?;
        let (first_result, second_result) = tokio::join!(
            self.submit_bundle_leg(first),
            self.submit_bundle_leg(second)
        );
        Ok(BundleSubmitReport::from_results(
            &command,
            BundleSubmissionPath::Concurrent,
            vec![first_result, second_result],
            Utc::now(),
        ))
    }

    async fn submit_bundle_batch(
        &self,
        command: BundleSubmitCommand,
        exchange: ExchangeId,
    ) -> Result<BundleSubmitReport> {
        let batch_ack = match self
            .route_batch_orders(BatchPlaceCommand::new(
                exchange,
                command.legs.iter().map(|leg| leg.command.clone()),
                command.requested_at,
            ))
            .await
        {
            Ok(ack) => ack,
            Err(error) => {
                let results = command
                    .legs
                    .iter()
                    .cloned()
                    .map(|leg| BundleLegSubmitResult::failed(leg, error.to_string()))
                    .collect();
                return Ok(BundleSubmitReport::from_results(
                    &command,
                    BundleSubmissionPath::Batch,
                    results,
                    Utc::now(),
                ));
            }
        };
        let mut results = Vec::with_capacity(command.legs.len());
        for leg in command.legs.iter().cloned() {
            if let Some(ack) = batch_ack
                .order_acks
                .iter()
                .find(|ack| ack.client_order_id == leg.command.client_order_id)
                .cloned()
            {
                results.push(BundleLegSubmitResult::acked(leg, ack));
                continue;
            }
            if let Some(error) = batch_ack
                .failed_orders
                .iter()
                .find(|error| error.order.client_order_id == leg.command.client_order_id)
            {
                results.push(BundleLegSubmitResult::failed(
                    leg,
                    format!(
                        "{}{}",
                        error
                            .code
                            .as_ref()
                            .map(|code| format!("{code}: "))
                            .unwrap_or_default(),
                        error.message
                    ),
                ));
                continue;
            }
            results.push(BundleLegSubmitResult::failed(
                leg,
                "batch-place response did not include this bundle leg",
            ));
        }

        Ok(BundleSubmitReport::from_results(
            &command,
            BundleSubmissionPath::Batch,
            results,
            batch_ack.acknowledged_at,
        ))
    }

    async fn submit_bundle_leg(
        &self,
        leg: crate::execution::BundleOrderLeg,
    ) -> BundleLegSubmitResult {
        match self.route_order(leg.command.clone()).await {
            Ok(ack) => BundleLegSubmitResult::acked(leg, ack),
            Err(error) => BundleLegSubmitResult::failed(leg, error.to_string()),
        }
    }

    pub async fn route_cancel(&self, command: CancelCommand) -> Result<CancelAck> {
        if self.dry_run {
            return Ok(CancelAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.cancel_order(command).await
    }

    pub async fn route_get_order(
        &self,
        query: crate::execution::OrderQuery,
    ) -> Result<crate::execution::OrderState> {
        let adapter = self.adapters.get(&query.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                query.exchange.as_str()
            )
        })?;
        adapter.get_order(query).await
    }

    pub async fn route_cancel_all(&self, command: CancelAllCommand) -> Result<CancelAllAck> {
        if self.dry_run {
            return Ok(CancelAllAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.cancel_all_orders(command).await
    }

    pub async fn route_cancel_batch(&self, command: CancelBatchCommand) -> Result<CancelBatchAck> {
        if self.dry_run {
            return Ok(CancelBatchAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.cancel_batch_orders(command).await
    }

    pub async fn route_amend_order(&self, command: AmendOrderCommand) -> Result<AmendOrderAck> {
        if self.dry_run {
            return Ok(AmendOrderAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.amend_order(command).await
    }

    pub async fn route_set_leverage(&self, command: LeverageCommand) -> Result<LeverageAck> {
        if self.dry_run {
            return Ok(LeverageAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.set_leverage(command).await
    }

    pub async fn route_set_position_mode(
        &self,
        command: PositionModeCommand,
    ) -> Result<PositionModeAck> {
        if self.dry_run {
            return Ok(PositionModeAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.set_position_mode(command).await
    }

    pub async fn route_close_position(
        &self,
        command: ClosePositionCommand,
    ) -> Result<ClosePositionAck> {
        if self.dry_run {
            return Ok(ClosePositionAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.close_position(command).await
    }

    pub async fn route_set_countdown_cancel_all(
        &self,
        command: CountdownCancelAllCommand,
    ) -> Result<CountdownCancelAllAck> {
        if self.dry_run {
            return Ok(CountdownCancelAllAck::dry_run(&command, Utc::now()));
        }

        let adapter = self.adapters.get(&command.exchange).ok_or_else(|| {
            anyhow!(
                "missing trading adapter for exchange {}",
                command.exchange.as_str()
            )
        })?;
        adapter.set_countdown_cancel_all(command).await
    }
}
