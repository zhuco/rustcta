use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{Duration, Instant};

use crate::core::types::{BatchOrderRequest, ExecutionReport, MarketType, OrderSide};
use crate::cta::account_manager::AccountInfo;

use super::engine::OrderDraft;

#[derive(Debug, Clone)]
pub struct RoutedExecutionReport {
    pub report: ExecutionReport,
    pub ws_receive_at: Instant,
}

#[derive(Default)]
pub struct AccountUserStreamRouter {
    symbol_routes: Mutex<HashMap<String, Vec<mpsc::UnboundedSender<RoutedExecutionReport>>>>,
    unknown_reports: Mutex<VecDeque<RoutedExecutionReport>>,
}

impl AccountUserStreamRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn subscribe(&self, symbol: &str) -> mpsc::UnboundedReceiver<RoutedExecutionReport> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.symbol_routes
            .lock()
            .await
            .entry(normalize_symbol(symbol))
            .or_default()
            .push(tx);
        rx
    }

    pub async fn route_execution_report(&self, report: ExecutionReport, ws_receive_at: Instant) {
        let key = normalize_symbol(&report.symbol);
        let event = RoutedExecutionReport {
            report,
            ws_receive_at,
        };
        let mut routes = self.symbol_routes.lock().await;
        if let Some(subscribers) = routes.get_mut(&key) {
            subscribers.retain(|tx| tx.send(event.clone()).is_ok());
            return;
        }
        drop(routes);

        let mut unknown = self.unknown_reports.lock().await;
        unknown.push_back(event);
        while unknown.len() > 1000 {
            unknown.pop_front();
        }
    }

    pub async fn unknown_len(&self) -> usize {
        self.unknown_reports.lock().await.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrderPriority {
    FillRepair = 0,
    Reconcile = 1,
    Maintenance = 2,
}

#[derive(Debug, Clone)]
pub struct SharedOrderRequest {
    pub symbol: String,
    pub priority: OrderPriority,
    pub drafts: Vec<OrderDraft>,
    pub market_type: MarketType,
}

#[derive(Clone)]
pub struct SharedOrderExecutor {
    account: Arc<AccountInfo>,
    min_interval: Duration,
    last_submit: Arc<Mutex<Option<Instant>>>,
}

impl SharedOrderExecutor {
    pub fn new(account: Arc<AccountInfo>, min_interval: Duration) -> Self {
        Self {
            account,
            min_interval,
            last_submit: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn submit_batch(
        &self,
        request: SharedOrderRequest,
    ) -> Result<crate::core::types::BatchOrderResponse> {
        self.throttle().await;
        let orders = request
            .drafts
            .iter()
            .map(|draft| build_order_request(&request.symbol, request.market_type, draft))
            .collect();
        let batch = BatchOrderRequest {
            orders,
            market_type: request.market_type,
        };
        self.account
            .exchange
            .create_batch_orders(batch)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn throttle(&self) {
        let mut last_submit = self.last_submit.lock().await;
        if let Some(last) = *last_submit {
            let elapsed = last.elapsed();
            if elapsed < self.min_interval {
                tokio::time::sleep(self.min_interval - elapsed).await;
            }
        }
        *last_submit = Some(Instant::now());
    }
}

fn build_order_request(
    symbol: &str,
    market_type: MarketType,
    draft: &OrderDraft,
) -> crate::core::types::OrderRequest {
    let mut params = HashMap::new();
    params.insert(
        "positionSide".to_string(),
        draft.intent.position_side().as_str().to_string(),
    );

    let mut request = crate::core::types::OrderRequest::new(
        symbol.to_string(),
        draft.intent.side(),
        crate::core::types::OrderType::Limit,
        draft.qty,
        Some(draft.price),
        market_type,
    );
    request.client_order_id = Some(draft.id.clone());
    request.post_only = Some(draft.post_only);
    if draft.post_only {
        request.time_in_force = Some("GTX".to_string());
    }
    request.params = Some(params);
    request
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::OrderStatus;
    use chrono::Utc;

    fn report(symbol: &str) -> ExecutionReport {
        ExecutionReport {
            order_id: "1".to_string(),
            client_order_id: Some("grid-1".to_string()),
            symbol: symbol.to_string(),
            side: OrderSide::Buy,
            order_type: crate::core::types::OrderType::Limit,
            status: OrderStatus::Open,
            price: 1.0,
            amount: 1.0,
            executed_amount: 0.0,
            executed_price: 0.0,
            commission: 0.0,
            commission_asset: String::new(),
            timestamp: Utc::now(),
            is_maker: true,
        }
    }

    #[tokio::test]
    async fn router_should_deliver_by_normalized_symbol() {
        let router = AccountUserStreamRouter::new();
        let mut rx = router.subscribe("ETH/USDC").await;
        router
            .route_execution_report(report("ETHUSDC"), Instant::now())
            .await;
        let routed = rx.try_recv().expect("routed report");
        assert_eq!(routed.report.symbol, "ETHUSDC");
        assert_eq!(router.unknown_len().await, 0);
    }

    #[tokio::test]
    async fn router_should_quarantine_unknown_symbol() {
        let router = AccountUserStreamRouter::new();
        router
            .route_execution_report(report("BTCUSDC"), Instant::now())
            .await;
        assert_eq!(router.unknown_len().await, 1);
    }
}
