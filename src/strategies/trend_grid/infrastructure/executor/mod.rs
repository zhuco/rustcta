use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;

use crate::core::types::Result as CoreResult;
use crate::core::types::{BatchOrderResponse, MarketType, Order, OrderRequest, OrderStatus};
use crate::cta::account_manager::AccountManager;
use crate::strategies::common::{OrderExecutionEvent, OrderExecutor};
use crate::utils::generate_order_id;

/// 执行模式
#[derive(Debug, Clone, Copy)]
pub enum ExecutionMode {
    Real,
    DryRun,
    Mock,
}

/// 基于 AccountManager 的订单执行器
pub struct AccountOrderExecutor {
    account_manager: Arc<AccountManager>,
    mode: ExecutionMode,
}

impl AccountOrderExecutor {
    pub fn new(account_manager: Arc<AccountManager>, mode: ExecutionMode) -> Self {
        Self {
            account_manager,
            mode,
        }
    }

    fn simulate_response(&self, orders: Vec<OrderRequest>) -> BatchOrderResponse {
        let successful_orders = orders
            .into_iter()
            .map(|req| Order {
                id: generate_order_id("mock", "sim"),
                symbol: req.symbol.clone(),
                side: req.side,
                order_type: req.order_type,
                amount: req.amount,
                price: req.price,
                filled: 0.0,
                remaining: req.amount,
                status: OrderStatus::Open,
                market_type: req.market_type,
                timestamp: Utc::now(),
                last_trade_timestamp: None,
                info: json!({
                    "simulated": true,
                    "client_order_id": req.client_order_id,
                }),
            })
            .collect();

        BatchOrderResponse {
            successful_orders,
            failed_orders: Vec::new(),
        }
    }
}

#[async_trait]
impl OrderExecutor for AccountOrderExecutor {
    async fn execute_batch(
        &self,
        account_id: &str,
        market_type: MarketType,
        orders: Vec<OrderRequest>,
    ) -> CoreResult<(BatchOrderResponse, OrderExecutionEvent)> {
        let request_count = orders.len();
        let response = match self.mode {
            ExecutionMode::Real => {
                self.account_manager
                    .create_batch_orders(account_id, orders)
                    .await?
            }
            ExecutionMode::DryRun | ExecutionMode::Mock => self.simulate_response(orders),
        };

        let event = OrderExecutionEvent::new(
            account_id,
            market_type,
            request_count,
            response.successful_orders.len(),
            response.failed_orders.len(),
        );

        Ok((response, event))
    }

    async fn cancel_all(
        &self,
        account_id: &str,
        symbol: Option<&str>,
        _market_type: MarketType,
    ) -> CoreResult<usize> {
        match self.mode {
            ExecutionMode::Real => {
                let cancelled = self
                    .account_manager
                    .cancel_all_orders(account_id, symbol)
                    .await?;
                Ok(cancelled.len())
            }
            ExecutionMode::DryRun | ExecutionMode::Mock => {
                log::info!(
                    "[{} executor] 模拟取消订单 account={} symbol={:?}",
                    self.name(),
                    account_id,
                    symbol
                );
                Ok(0)
            }
        }
    }

    fn name(&self) -> &'static str {
        match self.mode {
            ExecutionMode::Real => "real",
            ExecutionMode::DryRun => "dry_run",
            ExecutionMode::Mock => "mock",
        }
    }
}
