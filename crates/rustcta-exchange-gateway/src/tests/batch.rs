use super::*;
use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, BatchPlaceOrdersRequest,
    CapabilitySupport, OrderState, ReconcilePlan,
};

fn batch_capability(mode: BatchExecutionMode, max_items: Option<u32>) -> BatchCapability {
    BatchCapability {
        support: CapabilitySupport::native(),
        mode,
        atomicity: BatchAtomicity::Partial,
        max_items,
        same_symbol_required: false,
        same_market_type_required: false,
        supports_client_order_id: true,
        supports_partial_failure: true,
    }
}

fn place_request(client_order_id: &str) -> PlaceOrderRequest {
    match place_order_request(client_order_id).payload {
        GatewayRequestPayload::PlaceOrder(mut request) => {
            request.client_order_id = Some(client_order_id.to_string());
            request
        }
        other => panic!("unexpected payload: {other:?}"),
    }
}

fn order_from_request(request: &PlaceOrderRequest) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: request.symbol.exchange.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: Some("ex-1".to_string()),
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::Open,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

#[test]
fn batch_planner_should_chunk_declared_native_batch_limits() {
    let planner = BatchPlanner::new(
        exchange_id(),
        batch_capability(BatchExecutionMode::Native, Some(2)),
        BatchCapability::unsupported("not needed"),
    );
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-plan"),
        exchange: exchange_id(),
        orders: vec![
            place_request("cli-1"),
            place_request("cli-2"),
            place_request("cli-3"),
        ],
    };

    let plan = planner.plan_place(&request).expect("batch plan");

    assert!(plan.is_native());
    assert!(plan.must_report_item_results());
    assert_eq!(plan.chunks.len(), 2);
    assert_eq!(plan.chunks[0].item_indices, vec![0, 1]);
    assert_eq!(plan.chunks[1].item_indices, vec![2]);
}

#[test]
fn batch_planner_should_reject_unsupported_or_unsafe_client_ids() {
    let unsupported = BatchPlanner::new(
        exchange_id(),
        BatchCapability::unsupported("not supported"),
        BatchCapability::unsupported("not supported"),
    );
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-plan"),
        exchange: exchange_id(),
        orders: vec![place_request("cli-1")],
    };

    assert!(matches!(
        unsupported.plan_place(&request),
        Err(GatewayError::UnsupportedOperation { .. })
    ));

    let mut no_client_id = batch_capability(BatchExecutionMode::ComposedSequential, Some(10));
    no_client_id.supports_client_order_id = false;
    let planner = BatchPlanner::new(
        exchange_id(),
        no_client_id,
        BatchCapability::unsupported("not needed"),
    );

    assert!(matches!(
        planner.plan_place(&request),
        Err(GatewayError::InvalidPayload { .. })
    ));
}

#[test]
fn batch_planner_should_report_missing_items_as_reconciliation() {
    let planner = BatchPlanner::new(
        exchange_id(),
        batch_capability(BatchExecutionMode::Native, Some(10)),
        BatchCapability::unsupported("not needed"),
    );
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-plan"),
        exchange: exchange_id(),
        orders: vec![place_request("cli-1"), place_request("cli-2")],
    };
    let mut returned = order_from_request(&request.orders[0]);
    returned.client_order_id = Some("cli-1".to_string());

    let report = planner.missing_place_results(&request, &[returned]);

    assert_eq!(report.total_items, 2);
    assert_eq!(report.failed_count(), 1);
    assert!(report.requires_reconciliation());
    assert_eq!(report.results[0].index, 1);
    assert_eq!(report.results[0].client_order_id.as_deref(), Some("cli-2"));
    assert!(report.results[0]
        .reconcile_plan
        .as_ref()
        .is_some_and(ReconcilePlan::query_first));
}
