use super::*;

#[test]
fn admin_audit_plan_should_only_emit_readonly_checks() {
    let mut capabilities = rustcta_exchange_api::ExchangeClientCapabilities::new(exchange_id());
    capabilities.supports_public_rest = true;
    capabilities.supports_private_rest = true;
    capabilities.supports_symbol_rules = true;
    capabilities.supports_order_book_snapshot = true;
    capabilities.supports_balances = true;
    capabilities.supports_positions = true;
    capabilities.supports_fees = true;
    capabilities.supports_open_orders = true;
    capabilities.supports_recent_fills = true;
    capabilities.supports_place_order = true;
    capabilities.supports_cancel_order = true;
    capabilities.supports_batch_place_order = true;
    capabilities.supports_cancel_all_orders = true;

    let plan = AdminAuditPlanner::new().plan(&capabilities, &[symbol_scope()]);

    assert!(plan.safety.mutation_forbidden);
    assert!(plan.safety.read_only_guard_required);
    assert!(!plan.contains_mutation());
    assert!(plan.operation_names().contains(&"get_balances"));
    assert!(plan.operation_names().contains(&"get_recent_fills"));
    for forbidden in [
        "place_order",
        "cancel_order",
        "batch_place_orders",
        "cancel_all_orders",
    ] {
        assert!(!plan.operation_names().contains(&forbidden));
    }
}

#[test]
fn admin_audit_plan_should_skip_private_checks_without_capability() {
    let mut capabilities = rustcta_exchange_api::ExchangeClientCapabilities::new(exchange_id());
    capabilities.supports_public_rest = true;
    capabilities.supports_symbol_rules = true;
    capabilities.supports_order_book_snapshot = true;

    let plan = AdminAuditPlanner::new().plan(&capabilities, &[symbol_scope()]);

    assert!(plan.operation_names().contains(&"get_symbol_rules"));
    assert!(plan.operation_names().contains(&"get_order_book"));
    assert!(!plan.operation_names().contains(&"get_balances"));
    assert!(plan
        .skipped_private_checks
        .contains(&AdminAuditCheckKind::GetBalances));
    assert!(plan
        .skipped_private_checks
        .contains(&AdminAuditCheckKind::GetOpenOrders));
}

#[test]
fn admin_audit_plan_should_skip_private_checks_without_private_rest() {
    let mut capabilities = rustcta_exchange_api::ExchangeClientCapabilities::new(exchange_id());
    capabilities.supports_public_rest = true;
    capabilities.supports_private_rest = false;
    capabilities.supports_balances = true;
    capabilities.supports_positions = true;
    capabilities.supports_open_orders = true;

    let plan = AdminAuditPlanner::new().plan(&capabilities, &[symbol_scope()]);

    assert!(!plan.operation_names().contains(&"get_balances"));
    assert!(!plan.operation_names().contains(&"get_positions"));
    assert!(!plan.operation_names().contains(&"get_open_orders"));
    assert!(plan
        .skipped_private_checks
        .contains(&AdminAuditCheckKind::GetBalances));
    assert!(plan
        .skipped_private_checks
        .contains(&AdminAuditCheckKind::GetPositions));
    assert!(plan
        .skipped_private_checks
        .contains(&AdminAuditCheckKind::GetOpenOrders));
}
