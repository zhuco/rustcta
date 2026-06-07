use super::*;
use rustcta_exchange_api::{
    ClientOrderIdPolicy, ExchangeClientCapabilities, ExchangeErrorKind, IdempotencyKey,
    ReconcileTrigger, RetryReconcilePolicy,
};

#[test]
fn reconciliation_planner_should_make_timeout_query_first_without_replay() {
    let planner = ReconciliationPlanner::for_exchange(exchange_id());
    let plan = planner
        .plan_for_error(
            ReconcileTrigger::PlaceOrderTimeout,
            ExchangeErrorKind::Timeout,
            Some(symbol_scope()),
            Some(IdempotencyKey {
                client_order_id: Some("cli-1".to_string()),
                exchange_order_id: None,
                request_id: Some("req-1".to_string()),
            }),
            "place order transport timeout",
        )
        .expect("reconcile plan");

    assert!(plan.query_first());
    assert!(!plan.allow_order_replay);
    assert!(plan.requires_query_order);
    assert_eq!(
        plan.unknown_order_policy,
        rustcta_exchange_api::UnknownOrderPolicy::QueryByClientOrderId
    );
}

#[test]
fn reconciliation_planner_should_gate_live_dry_run_readbacks() {
    let planner = ReconciliationPlanner::new(ReconciliationPlannerConfig {
        exchange: exchange_id(),
        retry_policy: RetryReconcilePolicy::default(),
        client_order_id_policy: ClientOrderIdPolicy::default(),
        require_query_or_open_orders_for_live_dry_run: true,
    });
    let mut capabilities = ExchangeClientCapabilities::new(exchange_id());
    capabilities.supports_private_rest = true;

    assert!(matches!(
        planner.validate_live_dry_run_capabilities(&capabilities),
        Err(GatewayError::UnsupportedOperation { .. })
    ));

    capabilities.supports_open_orders = true;
    planner
        .validate_live_dry_run_capabilities(&capabilities)
        .expect("open orders provides reconciliation readback");
}

#[test]
fn reconciliation_planner_should_classify_observed_order_status() {
    let planner = ReconciliationPlanner::for_exchange(exchange_id());

    assert_eq!(
        planner.classify_observed_status(Some(OrderStatus::Filled), false, false),
        rustcta_exchange_api::OrderReconcileState::FoundFilled
    );
    assert_eq!(
        planner.classify_observed_status(None, true, false),
        rustcta_exchange_api::OrderReconcileState::FoundOpen
    );
    assert_eq!(
        planner.classify_observed_status(None, false, false),
        rustcta_exchange_api::OrderReconcileState::MissingNeedsOpenOrders
    );
}
