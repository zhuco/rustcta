#![allow(clippy::items_after_test_module)]

use crate::execution::{
    AmendOrderAck, AmendOrderCommand, CancelAck, CancelAllAck, CancelAllCommand, CancelBatchAck,
    CancelBatchCommand, CancelCommand, ClosePositionAck, ClosePositionCommand,
    CountdownCancelAllAck, CountdownCancelAllCommand, LeverageAck, LeverageCommand, OrderAck,
    OrderCommand, PositionModeAck, PositionModeCommand, TradingAdapter,
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
        CountdownCancelAllCommand, ExchangeBalance, ExchangePosition, OrderCommandStatus,
        OrderQuery, OrderState, PositionMode, TradingCapabilities,
    };
    use crate::market::{CanonicalSymbol, ExchangeSymbol};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
