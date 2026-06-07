use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, BatchItemResult, BatchOperationReport,
    BatchPlaceOrdersRequest, CancelOrderRequest, ExchangeError, ExchangeErrorKind, ExchangeId,
    IdempotencyKey, PlaceOrderRequest, ReconcilePlan, ReconcileTrigger, RetryReconcilePolicy,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeErrorClass;

use crate::GatewayError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOperationKind {
    PlaceOrders,
    CancelOrders,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchPlanner {
    pub exchange: ExchangeId,
    pub place: BatchCapability,
    pub cancel: BatchCapability,
    pub retry_policy: RetryReconcilePolicy,
}

impl BatchPlanner {
    pub fn new(exchange: ExchangeId, place: BatchCapability, cancel: BatchCapability) -> Self {
        Self {
            exchange,
            place,
            cancel,
            retry_policy: RetryReconcilePolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryReconcilePolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn plan_place(
        &self,
        request: &BatchPlaceOrdersRequest,
    ) -> Result<BatchExecutionPlan, GatewayError> {
        if request.exchange != self.exchange {
            return Err(GatewayError::InvalidPayload {
                message: format!(
                    "batch place exchange mismatch: request={} planner={}",
                    request.exchange, self.exchange
                ),
            });
        }
        let items = request
            .orders
            .iter()
            .enumerate()
            .map(|(index, order)| BatchPlanItem {
                index,
                symbol: order.symbol.clone(),
                idempotency_key: IdempotencyKey::from_place_request(order),
            })
            .collect::<Vec<_>>();
        self.plan(BatchOperationKind::PlaceOrders, &self.place, items)
    }

    pub fn plan_cancel(
        &self,
        request: &rustcta_exchange_api::BatchCancelOrdersRequest,
    ) -> Result<BatchExecutionPlan, GatewayError> {
        if request.exchange != self.exchange {
            return Err(GatewayError::InvalidPayload {
                message: format!(
                    "batch cancel exchange mismatch: request={} planner={}",
                    request.exchange, self.exchange
                ),
            });
        }
        let items = request
            .cancels
            .iter()
            .enumerate()
            .map(|(index, cancel)| BatchPlanItem {
                index,
                symbol: cancel.symbol.clone(),
                idempotency_key: IdempotencyKey::from_cancel_request(cancel),
            })
            .collect::<Vec<_>>();
        self.plan(BatchOperationKind::CancelOrders, &self.cancel, items)
    }

    fn plan(
        &self,
        kind: BatchOperationKind,
        capability: &BatchCapability,
        items: Vec<BatchPlanItem>,
    ) -> Result<BatchExecutionPlan, GatewayError> {
        if items.is_empty() {
            return Err(GatewayError::InvalidPayload {
                message: "batch operation requires at least one item".to_string(),
            });
        }
        if !capability.support.is_supported()
            || matches!(capability.mode, BatchExecutionMode::Unsupported)
        {
            return Err(GatewayError::UnsupportedOperation {
                operation: format!("{}.{}", self.exchange, kind.as_str()),
            });
        }
        if capability.same_symbol_required && !same_symbol(&items) {
            return Err(GatewayError::InvalidPayload {
                message: "batch operation requires all items to share one symbol".to_string(),
            });
        }
        if capability.same_market_type_required && !same_market_type(&items) {
            return Err(GatewayError::InvalidPayload {
                message: "batch operation requires all items to share one market type".to_string(),
            });
        }
        if !capability.supports_client_order_id
            && kind == BatchOperationKind::PlaceOrders
            && items
                .iter()
                .any(|item| item.idempotency_key.client_order_id.is_some())
        {
            return Err(GatewayError::InvalidPayload {
                message: "batch place capability does not accept client order ids".to_string(),
            });
        }

        let max_items = capability
            .max_items
            .map(|value| value as usize)
            .unwrap_or(items.len())
            .max(1);
        let batches = items
            .chunks(max_items)
            .map(|chunk| BatchExecutionChunk {
                mode: capability.mode,
                atomicity: capability.atomicity,
                item_indices: chunk.iter().map(|item| item.index).collect(),
            })
            .collect();

        Ok(BatchExecutionPlan {
            exchange: self.exchange.clone(),
            kind,
            mode: capability.mode,
            atomicity: capability.atomicity,
            supports_partial_failure: capability.supports_partial_failure,
            chunks: batches,
            retry_policy: self.retry_policy.clone(),
        })
    }

    pub fn missing_place_results(
        &self,
        request: &BatchPlaceOrdersRequest,
        returned_orders: &[rustcta_exchange_api::OrderState],
    ) -> BatchOperationReport {
        let mut matched = vec![false; request.orders.len()];
        for order in returned_orders {
            if let Some(index) = request
                .orders
                .iter()
                .position(|request| order_matches_place_request(order, request))
            {
                matched[index] = true;
            }
        }

        let results = request
            .orders
            .iter()
            .enumerate()
            .filter(|(index, _)| !matched[*index])
            .map(|(index, order)| {
                let plan = ReconcilePlan::for_place_request(
                    self.exchange.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    order,
                    self.retry_policy.clone(),
                    "batch place response did not include this item",
                );
                BatchItemResult::failed(
                    index,
                    order.client_order_id.clone(),
                    None,
                    reconciliation_error(&self.exchange, order.client_order_id.clone(), None),
                    Some(plan),
                )
            })
            .collect::<Vec<_>>();

        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: self.exchange.clone(),
            total_items: request.orders.len(),
            results,
        }
    }

    pub fn missing_cancel_results(
        &self,
        request: &rustcta_exchange_api::BatchCancelOrdersRequest,
        returned_orders: &[rustcta_exchange_api::OrderState],
    ) -> BatchOperationReport {
        let mut matched = vec![false; request.cancels.len()];
        for order in returned_orders {
            if let Some(index) = request
                .cancels
                .iter()
                .position(|request| order_matches_cancel_request(order, request))
            {
                matched[index] = true;
            }
        }

        let results = request
            .cancels
            .iter()
            .enumerate()
            .filter(|(index, _)| !matched[*index])
            .map(|(index, cancel)| {
                let plan = ReconcilePlan::for_cancel_request(
                    self.exchange.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    cancel,
                    self.retry_policy.clone(),
                    "batch cancel response did not include this item",
                );
                BatchItemResult::failed(
                    index,
                    cancel.client_order_id.clone(),
                    cancel.exchange_order_id.clone(),
                    reconciliation_error(
                        &self.exchange,
                        cancel.client_order_id.clone(),
                        cancel.exchange_order_id.clone(),
                    ),
                    Some(plan),
                )
            })
            .collect::<Vec<_>>();

        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: self.exchange.clone(),
            total_items: request.cancels.len(),
            results,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchExecutionPlan {
    pub exchange: ExchangeId,
    pub kind: BatchOperationKind,
    pub mode: BatchExecutionMode,
    pub atomicity: BatchAtomicity,
    pub supports_partial_failure: bool,
    pub chunks: Vec<BatchExecutionChunk>,
    pub retry_policy: RetryReconcilePolicy,
}

impl BatchExecutionPlan {
    pub fn is_native(&self) -> bool {
        matches!(self.mode, BatchExecutionMode::Native)
    }

    pub fn must_report_item_results(&self) -> bool {
        !matches!(self.atomicity, BatchAtomicity::Atomic) || self.supports_partial_failure
    }
}

impl BatchOperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlaceOrders => "batch_place_orders",
            Self::CancelOrders => "batch_cancel_orders",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchExecutionChunk {
    pub mode: BatchExecutionMode,
    pub atomicity: BatchAtomicity,
    pub item_indices: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq)]
struct BatchPlanItem {
    index: usize,
    symbol: SymbolScope,
    idempotency_key: IdempotencyKey,
}

fn same_symbol(items: &[BatchPlanItem]) -> bool {
    items.first().map_or(true, |first| {
        items.iter().all(|item| item.symbol == first.symbol)
    })
}

fn same_market_type(items: &[BatchPlanItem]) -> bool {
    items.first().map_or(true, |first| {
        items
            .iter()
            .all(|item| item.symbol.market_type == first.symbol.market_type)
    })
}

fn order_matches_place_request(
    order: &rustcta_exchange_api::OrderState,
    request: &PlaceOrderRequest,
) -> bool {
    order.client_order_id.is_some()
        && order.client_order_id == request.client_order_id
        && order.exchange == request.symbol.exchange
        && order.exchange_symbol == request.symbol.exchange_symbol
}

fn order_matches_cancel_request(
    order: &rustcta_exchange_api::OrderState,
    request: &CancelOrderRequest,
) -> bool {
    order.exchange == request.symbol.exchange
        && order.exchange_symbol == request.symbol.exchange_symbol
        && ((request.client_order_id.is_some() && order.client_order_id == request.client_order_id)
            || (request.exchange_order_id.is_some()
                && order.exchange_order_id == request.exchange_order_id))
}

fn reconciliation_error(
    exchange: &ExchangeId,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
) -> ExchangeError {
    let mut error = ExchangeError::new(
        exchange.clone(),
        ExchangeErrorClass::UnknownOrderState,
        format!(
            "{:?} requires reconciliation",
            ExchangeErrorKind::UnknownOrderState
        ),
        chrono::Utc::now(),
    );
    error.client_order_id = client_order_id;
    error.order_id = exchange_order_id;
    error
}
