use async_trait::async_trait;

use crate::core::types::{BatchOrderResponse, MarketType, OrderRequest, Result as CoreResult};

/// 执行过程中产生的事件（用于日志和回调）
#[derive(Debug, Clone)]
pub struct OrderExecutionEvent {
    pub account_id: String,
    pub market_type: MarketType,
    pub request_count: usize,
    pub success_count: usize,
    pub fail_count: usize,
}

impl OrderExecutionEvent {
    pub fn new(
        account_id: impl Into<String>,
        market_type: MarketType,
        request_count: usize,
        success_count: usize,
        fail_count: usize,
    ) -> Self {
        Self {
            account_id: account_id.into(),
            market_type,
            request_count,
            success_count,
            fail_count,
        }
    }
}

/// 统一的订单执行接口，支持真实、Dry-run、Mock 等多种模式
#[async_trait]
pub trait OrderExecutor: Send + Sync {
    /// 执行一批订单请求
    async fn execute_batch(
        &self,
        account_id: &str,
        market_type: MarketType,
        orders: Vec<OrderRequest>,
    ) -> CoreResult<(BatchOrderResponse, OrderExecutionEvent)>;

    /// 取消指定账户的所有订单
    async fn cancel_all(
        &self,
        account_id: &str,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> CoreResult<usize>;

    /// 返回执行器类型，用于日志
    fn name(&self) -> &'static str;
}
