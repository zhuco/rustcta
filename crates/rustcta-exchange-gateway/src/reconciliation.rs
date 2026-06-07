use rustcta_exchange_api::{
    ClientOrderIdPolicy, ExchangeClientCapabilities, ExchangeErrorKind, ExchangeId, IdempotencyKey,
    OrderReconcileState, OrderStatus, ReconcilePlan, ReconcileTrigger, RetryReconcilePolicy,
    UnknownOrderPolicy,
};

use crate::GatewayError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationPlannerConfig {
    pub exchange: ExchangeId,
    pub retry_policy: RetryReconcilePolicy,
    pub client_order_id_policy: ClientOrderIdPolicy,
    pub require_query_or_open_orders_for_live_dry_run: bool,
}

impl ReconciliationPlannerConfig {
    pub fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            retry_policy: RetryReconcilePolicy::default(),
            client_order_id_policy: ClientOrderIdPolicy::default(),
            require_query_or_open_orders_for_live_dry_run: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationPlanner {
    config: ReconciliationPlannerConfig,
}

impl ReconciliationPlanner {
    pub fn new(config: ReconciliationPlannerConfig) -> Self {
        Self { config }
    }

    pub fn for_exchange(exchange: ExchangeId) -> Self {
        Self::new(ReconciliationPlannerConfig::new(exchange))
    }

    pub fn plan_for_error(
        &self,
        trigger: ReconcileTrigger,
        error_kind: ExchangeErrorKind,
        symbol: Option<rustcta_exchange_api::SymbolScope>,
        idempotency_key: Option<IdempotencyKey>,
        reason: impl Into<String>,
    ) -> Option<ReconcilePlan> {
        if !error_kind.requires_reconciliation()
            && !matches!(error_kind, ExchangeErrorKind::Timeout)
        {
            return None;
        }
        Some(self.plan_from_key(trigger, symbol, idempotency_key, reason))
    }

    pub fn plan_from_key(
        &self,
        trigger: ReconcileTrigger,
        symbol: Option<rustcta_exchange_api::SymbolScope>,
        idempotency_key: Option<IdempotencyKey>,
        reason: impl Into<String>,
    ) -> ReconcilePlan {
        let mut plan = ReconcilePlan::from_key(
            self.config.exchange.clone(),
            trigger,
            symbol,
            idempotency_key,
            self.config.retry_policy.clone(),
            reason,
        );
        if plan.idempotency_key.as_ref().map_or(true, |key| {
            key.client_order_id.is_none() && key.exchange_order_id.is_none()
        }) {
            plan.requires_query_order = false;
            plan.requires_open_orders = true;
            plan.unknown_order_policy = UnknownOrderPolicy::CheckOpenOrders;
        }
        if self
            .config
            .client_order_id_policy
            .duplicate_requires_reconcile
            && matches!(trigger, ReconcileTrigger::DuplicateClientOrderId)
        {
            plan.allow_order_replay = false;
            plan.requires_query_order = true;
            plan.unknown_order_policy = UnknownOrderPolicy::QueryByClientOrderId;
        }
        plan
    }

    pub fn classify_observed_status(
        &self,
        status: Option<OrderStatus>,
        seen_in_open_orders: bool,
        seen_in_recent_fills: bool,
    ) -> OrderReconcileState {
        match status {
            Some(OrderStatus::Open) | Some(OrderStatus::PartiallyFilled) => {
                OrderReconcileState::FoundOpen
            }
            Some(OrderStatus::Filled) => OrderReconcileState::FoundFilled,
            Some(OrderStatus::Cancelled) => OrderReconcileState::FoundCancelled,
            Some(OrderStatus::Rejected) | Some(OrderStatus::Expired) => {
                OrderReconcileState::FoundRejected
            }
            Some(_) if seen_in_open_orders => OrderReconcileState::FoundOpen,
            Some(_) if seen_in_recent_fills => OrderReconcileState::FoundFilled,
            Some(_) => OrderReconcileState::UnknownNeedsRetry,
            None if seen_in_open_orders => OrderReconcileState::FoundOpen,
            None if seen_in_recent_fills => OrderReconcileState::MissingNeedsFills,
            None => OrderReconcileState::MissingNeedsOpenOrders,
        }
    }

    pub fn validate_live_dry_run_capabilities(
        &self,
        capabilities: &ExchangeClientCapabilities,
    ) -> Result<(), GatewayError> {
        if !capabilities.supports_private_rest {
            return Err(GatewayError::UnsupportedOperation {
                operation: format!("{}.private_rest", self.config.exchange),
            });
        }
        if self.config.require_query_or_open_orders_for_live_dry_run
            && !capabilities.supports_query_order
            && !capabilities.supports_open_orders
        {
            return Err(GatewayError::UnsupportedOperation {
                operation: format!("{}.reconciliation_readback", self.config.exchange),
            });
        }
        Ok(())
    }
}
