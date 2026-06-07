use chrono::Utc;
use rustcta_exchange_api::{
    BatchAtomicity, BatchCancelOrdersRequest, BatchCapability, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelOrderRequest, CapabilitySupport, ClientOrderIdPolicy,
    ExchangeClientCapabilities, ExchangeErrorKind, IdempotencyKey, OrderSide, OrderState,
    OrderStatus, OrderType, PlaceOrderRequest, ReconcilePlan, ReconcileTrigger, RequestContext,
    RetryReconcilePolicy, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    BatchPlanner, GatewayError, ReconciliationPlanner, ReconciliationPlannerConfig,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("binance").expect("exchange id")
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical symbol")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT")
            .expect("exchange symbol"),
    }
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: None,
        account_id: None,
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: Utc::now(),
    }
}

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
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(client_order_id),
        symbol: symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01000000".to_string(),
        price: Some("50000.00".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: true,
    }
}

fn cancel_request(client_order_id: &str, exchange_order_id: &str) -> CancelOrderRequest {
    CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(client_order_id),
        symbol: symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        exchange_order_id: Some(exchange_order_id.to_string()),
    }
}

fn cancel_request_for_symbol(
    client_order_id: &str,
    exchange_order_id: &str,
    symbol: SymbolScope,
) -> CancelOrderRequest {
    CancelOrderRequest {
        symbol,
        ..cancel_request(client_order_id, exchange_order_id)
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
fn task7_batch_planner_should_chunk_declared_native_batch_limits() {
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
fn task7_batch_planner_should_plan_cancel_constraints_and_atomic_reporting() {
    let mut cancel_capability = batch_capability(BatchExecutionMode::Native, Some(2));
    cancel_capability.atomicity = BatchAtomicity::Atomic;
    cancel_capability.supports_partial_failure = false;
    cancel_capability.same_symbol_required = true;
    let planner = BatchPlanner::new(
        exchange_id(),
        BatchCapability::unsupported("not needed"),
        cancel_capability.clone(),
    );
    let request = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-cancel"),
        exchange: exchange_id(),
        cancels: vec![
            cancel_request("cli-1", "ex-1"),
            cancel_request("cli-2", "ex-2"),
            cancel_request("cli-3", "ex-3"),
        ],
    };

    let plan = planner.plan_cancel(&request).expect("cancel batch plan");

    assert!(plan.is_native());
    assert!(!plan.must_report_item_results());
    assert_eq!(plan.atomicity, BatchAtomicity::Atomic);
    assert_eq!(plan.chunks.len(), 2);
    assert_eq!(plan.chunks[0].item_indices, vec![0, 1]);
    assert_eq!(plan.chunks[1].item_indices, vec![2]);

    let mut other_symbol = symbol_scope();
    other_symbol.exchange_symbol =
        ExchangeSymbol::new(exchange_id(), MarketType::Spot, "ETHUSDT").expect("exchange symbol");
    let invalid = BatchCancelOrdersRequest {
        cancels: vec![
            cancel_request("cli-1", "ex-1"),
            cancel_request_for_symbol("cli-2", "ex-2", other_symbol),
        ],
        ..request
    };

    assert!(matches!(
        planner.plan_cancel(&invalid),
        Err(GatewayError::InvalidPayload { .. })
    ));
}

#[test]
fn task7_batch_planner_should_reject_unsupported_or_unsafe_client_ids() {
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
fn task7_batch_planner_should_reject_empty_and_exchange_mismatch() {
    let planner = BatchPlanner::new(
        exchange_id(),
        batch_capability(BatchExecutionMode::Native, Some(10)),
        BatchCapability::unsupported("not needed"),
    );
    let empty = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("empty"),
        exchange: exchange_id(),
        orders: Vec::new(),
    };
    assert!(matches!(
        planner.plan_place(&empty),
        Err(GatewayError::InvalidPayload { .. })
    ));

    let mismatch = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mismatch"),
        exchange: ExchangeId::new("okx").expect("exchange id"),
        orders: vec![place_request("cli-1")],
    };
    assert!(matches!(
        planner.plan_place(&mismatch),
        Err(GatewayError::InvalidPayload { .. })
    ));
}

#[test]
fn task7_batch_planner_should_report_missing_items_as_reconciliation() {
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
    let returned = order_from_request(&request.orders[0]);

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

#[test]
fn task7_batch_planner_should_report_missing_cancel_items_without_cross_symbol_match() {
    let planner = BatchPlanner::new(
        exchange_id(),
        BatchCapability::unsupported("not needed"),
        batch_capability(BatchExecutionMode::ComposedSequential, Some(10)),
    );
    let request = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-cancel"),
        exchange: exchange_id(),
        cancels: vec![
            cancel_request("cli-1", "ex-1"),
            cancel_request("cli-2", "ex-2"),
        ],
    };
    let mut returned = order_from_request(&place_request("cli-2"));
    returned.exchange_order_id = Some("ex-2".to_string());
    returned.exchange_symbol =
        ExchangeSymbol::new(exchange_id(), MarketType::Spot, "ETHUSDT").expect("exchange symbol");

    let report = planner.missing_cancel_results(&request, &[returned]);

    assert_eq!(report.total_items, 2);
    assert_eq!(report.failed_count(), 2);
    assert!(report.requires_reconciliation());
    assert_eq!(report.results[0].exchange_order_id.as_deref(), Some("ex-1"));
    assert_eq!(report.results[1].exchange_order_id.as_deref(), Some("ex-2"));
}

#[test]
fn task7_reconciliation_planner_should_make_timeout_query_first_without_replay() {
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
fn task7_reconciliation_planner_should_gate_live_dry_run_readbacks() {
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
