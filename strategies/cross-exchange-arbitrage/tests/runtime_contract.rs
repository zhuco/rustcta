use chrono::{TimeZone, Utc};
use rustcta_strategy_cross_exchange_arbitrage::app_runtime::{
    CrossArbAppRuntime, CrossArbAuthEvidence, CrossArbDbAuditEvent,
    CrossArbDbOpenDecisionAuditEvent, CrossArbDbPriceAuditEvent, CrossArbDbPriceAuditLeg,
    CrossArbRuntimeInput,
};
use rustcta_strategy_cross_exchange_arbitrage::runtime_contract::{
    build_runtime_contract, CrossArbExchangeReadinessRow,
};
use rustcta_strategy_cross_exchange_arbitrage::{
    CanonicalSymbol, CrossArbExecutionModule, CrossExchangeArbitrageConfig, ExchangeId,
    OpenOpportunityAudit, OpenOpportunityDecision, OpenOpportunityRejectReason,
};
use serde_json::json;

#[test]
fn runtime_contract_should_expose_db_takeover_singleton_and_concurrent_execution_contracts() {
    let contract = build_runtime_contract(&CrossExchangeArbitrageConfig::default(), fixed_now());

    assert!(contract.async_db_writer.required);
    assert!(
        contract
            .async_db_writer
            .enqueue_from_strategy_thread_must_be_non_blocking
    );
    assert!(contract.async_db_writer.background_writer_required);
    assert!(contract.async_db_writer.records_planned_execution_prices);
    assert!(contract.async_db_writer.records_actual_fill_prices);
    assert!(contract.async_db_writer.records_open_decision_reject_reason);
    assert!(contract.async_db_writer.records_open_decision_auth_evidence);
    assert!(contract
        .async_db_writer
        .price_audit_fields
        .contains(&"planned_execution_price"));
    assert!(contract
        .async_db_writer
        .price_audit_fields
        .contains(&"actual_fill_price"));
    assert!(contract
        .async_db_writer
        .price_audit_fields
        .contains(&"reject_reason"));
    assert!(contract
        .async_db_writer
        .price_audit_fields
        .contains(&"auth_evidence"));

    assert!(contract.position_takeover.required_before_new_open);
    assert_eq!(contract.position_takeover.quote_asset, "USDT");
    assert_eq!(
        contract.position_takeover.single_leg_resolution_policy,
        "market_reduce_only_close_before_new_open"
    );

    assert!(contract.singleton_process_lock.required);
    assert!(
        contract
            .singleton_process_lock
            .blocks_startup_until_acquired
    );
    assert_eq!(
        contract.singleton_process_lock.duplicate_process_policy,
        "abort_startup_when_existing_live_lock_is_present"
    );

    assert!(
        contract
            .execution_provider_contract
            .open_legs_concurrent_required
    );
    assert!(contract.execution_provider_contract.taker_only);
    assert!(
        contract
            .execution_provider_contract
            .supports_slippage_capture_maker_open
    );
    assert!(
        !contract
            .execution_provider_contract
            .maker_open_then_taker_hedge_required
    );
    assert!(
        contract
            .execution_provider_contract
            .close_legs_concurrent_required
    );
    assert!(
        contract
            .execution_provider_contract
            .must_return_planned_and_actual_prices
    );

    for gate in [
        "async_db_writer",
        "position_takeover",
        "singleton_process_lock",
    ] {
        assert!(contract.readiness_gate.required_gates.contains(&gate));
    }
    assert!(!contract
        .readiness_gate
        .required_gates
        .contains(&"startup_single_leg_resolution"));

    for task_kind in [
        "acquire_singleton_process_lock",
        "start_async_db_writer",
        "takeover_all_usdt_positions",
        "resolve_startup_single_leg_positions",
        "submit_dual_taker_open_legs_concurrently",
        "submit_dual_taker_close_legs_concurrently",
        "submit_slippage_capture_maker_open",
        "cancel_unfilled_slippage_capture_maker_open",
        "submit_slippage_capture_taker_hedge_after_fill",
        "persist_price_audit_events_from_async_queue",
    ] {
        assert!(
            contract
                .tasks
                .iter()
                .any(|task| task.task_kind == task_kind),
            "{task_kind} task should be advertised by the runtime contract"
        );
    }
}

#[test]
fn runtime_contract_should_switch_execution_provider_for_slippage_capture_module() {
    let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
        "execution_module": "slippage_capture",
        "slippage_capture": {
            "maker_order_timeout_ms": 750,
            "maker_price_offset_pct": 0.002
        }
    }));
    let contract = build_runtime_contract(&config, fixed_now());

    assert_eq!(
        contract.execution_module,
        CrossArbExecutionModule::SlippageCapture
    );
    assert_eq!(
        contract.dashboard_snapshot.execution_module,
        CrossArbExecutionModule::SlippageCapture
    );
    assert!(!contract.execution_provider_contract.taker_only);
    assert!(
        !contract
            .execution_provider_contract
            .open_legs_concurrent_required
    );
    assert!(
        contract
            .execution_provider_contract
            .maker_open_then_taker_hedge_required
    );
    assert!(
        contract
            .dashboard_snapshot
            .slippage_capture
            .enabled_when_selected
    );
    assert_eq!(
        contract
            .dashboard_snapshot
            .slippage_capture
            .maker_order_timeout_ms,
        750
    );
    assert_eq!(
        contract
            .dashboard_snapshot
            .slippage_capture
            .maker_price_offset_pct,
        "0.2"
    );
}

#[test]
fn app_runtime_should_block_new_orders_until_all_startup_gates_are_complete() {
    let captured_at = fixed_now();
    let mut runtime = CrossArbAppRuntime::default().with_live_orders_enabled(true);

    let blocked = runtime.run_cycle(
        CrossArbRuntimeInput {
            execution_intents: 2,
            exchange_readiness_rows: ready_exchanges(),
            db_writer_ready: false,
            position_takeover_complete: false,
            startup_single_leg_positions_detected: 1,
            startup_single_leg_positions_resolved: 0,
            singleton_acquired: false,
            ..Default::default()
        },
        captured_at,
    );

    assert_eq!(blocked.execution.requested_intents, 2);
    assert_eq!(blocked.execution.submitted_intents, 0);
    assert!(blocked.execution.blocked_by_startup_gate);
    assert!(
        !blocked
            .dashboard_snapshot
            .readiness_gate
            .ready_to_open_new_positions
    );
    assert!(blocked.notifications.iter().any(|notification| {
        notification.notification_kind == "startup_gate_blocked_execution"
    }));

    let ready = runtime.run_cycle(
        CrossArbRuntimeInput {
            execution_intents: 2,
            exchange_readiness_rows: ready_exchanges(),
            db_writer_ready: true,
            position_takeover_complete: true,
            startup_single_leg_positions_detected: 1,
            startup_single_leg_positions_resolved: 1,
            singleton_acquired: true,
            ..Default::default()
        },
        captured_at,
    );

    assert_eq!(ready.execution.submitted_intents, 2);
    assert!(ready.execution.startup_readiness_satisfied);
    assert!(ready.execution.new_opens_allowed);
    assert!(
        ready
            .dashboard_snapshot
            .readiness_gate
            .ready_to_evaluate_opportunities
    );
    assert!(ready.dashboard_snapshot.async_db_writer.writer_ready);
    assert!(ready.dashboard_snapshot.position_takeover.takeover_complete);
    assert!(ready.dashboard_snapshot.singleton_process_lock.acquired);
}

#[test]
fn app_runtime_should_not_block_new_opens_on_unresolved_startup_single_leg_positions() {
    let captured_at = fixed_now();
    let mut runtime = CrossArbAppRuntime::default().with_live_orders_enabled(true);

    let cycle = runtime.run_cycle(
        CrossArbRuntimeInput {
            execution_intents: 2,
            exchange_readiness_rows: ready_exchanges(),
            db_writer_ready: true,
            position_takeover_complete: true,
            startup_single_leg_positions_detected: 1,
            startup_single_leg_positions_resolved: 0,
            singleton_acquired: true,
            ..Default::default()
        },
        captured_at,
    );

    assert_eq!(cycle.execution.requested_intents, 2);
    assert_eq!(cycle.execution.submitted_intents, 2);
    assert!(cycle.execution.startup_readiness_satisfied);
    assert!(cycle.execution.new_opens_allowed);
    assert!(!cycle.execution.blocked_by_startup_gate);
    assert!(
        cycle
            .dashboard_snapshot
            .readiness_gate
            .ready_to_open_new_positions
    );
    assert!(
        !cycle
            .dashboard_snapshot
            .readiness_gate
            .startup_single_leg_positions_resolved
    );
    assert_eq!(
        cycle
            .dashboard_snapshot
            .position_takeover
            .single_leg_positions_detected,
        1
    );
    assert_eq!(
        cycle
            .dashboard_snapshot
            .position_takeover
            .single_leg_positions_resolved,
        0
    );
}

#[test]
fn app_runtime_should_enqueue_price_audit_events_without_blocking_strategy_cycle() {
    let captured_at = fixed_now();
    let mut runtime = CrossArbAppRuntime::default()
        .with_live_orders_enabled(true)
        .with_async_db_queue_capacity(1);

    let cycle = runtime.run_cycle(
        CrossArbRuntimeInput {
            exchange_readiness_rows: ready_exchanges(),
            db_writer_ready: true,
            position_takeover_complete: true,
            singleton_acquired: true,
            price_audit_events: vec![
                price_audit_event("bundle-1", "100.00", Some("100.02")),
                price_audit_event("bundle-2", "101.00", Some("101.01")),
            ],
            ..Default::default()
        },
        captured_at,
    );

    assert_eq!(cycle.async_db_queue.queued_events, 1);
    assert_eq!(cycle.async_db_queue.dropped_events, 1);
    assert!(cycle.async_db_queue.non_blocking_enqueue);
    assert_eq!(runtime.async_db_queue_len(), 1);
    assert_eq!(runtime.dropped_db_events(), 1);
    assert!(cycle.storage_events.iter().any(|event| {
        event.event_kind == "async_price_audit_events_enqueued" && event.count == 1
    }));
    assert!(cycle
        .notifications
        .iter()
        .any(|notification| { notification.notification_kind == "async_db_queue_overflow" }));

    let queued = &cycle.queued_price_audit_events[0];
    assert!(queued.non_blocking_enqueue_required);
    assert_eq!(queued.legs[0].planned_execution_price, "100.00");
    assert_eq!(queued.legs[0].actual_fill_price.as_deref(), Some("100.02"));

    let drained = runtime.drain_async_db_queue_batch(8);
    assert_eq!(drained.len(), 1);
    assert!(matches!(drained[0], CrossArbDbAuditEvent::Price(_)));
    assert_eq!(runtime.async_db_queue_len(), 0);
}

#[test]
fn app_runtime_should_enqueue_rejected_open_decision_with_auth_evidence() {
    let captured_at = fixed_now();
    let audit = open_decision_audit_event();
    let mut runtime = CrossArbAppRuntime::default()
        .with_live_orders_enabled(true)
        .with_async_db_queue_capacity(8);

    let cycle = runtime.run_cycle(
        CrossArbRuntimeInput {
            exchange_readiness_rows: ready_exchanges(),
            db_writer_ready: true,
            position_takeover_complete: true,
            singleton_acquired: true,
            open_decision_audit_events: vec![CrossArbDbAuditEvent::OpenDecision(audit)],
            ..Default::default()
        },
        captured_at,
    );

    assert_eq!(cycle.async_db_queue.queued_events, 1);
    assert_eq!(cycle.queued_db_audit_events.len(), 1);
    assert!(cycle
        .storage_events
        .iter()
        .any(|event| { event.event_kind == "open_decision_audit" && event.count == 1 }));

    let CrossArbDbAuditEvent::OpenDecision(event) = &cycle.queued_db_audit_events[0] else {
        panic!("open decision audit event should be queued");
    };
    assert!(!event.accepted);
    assert_eq!(
        event.reject_reason,
        Some(OpenOpportunityRejectReason::SymbolAlreadyActive)
    );
    assert!(event.non_blocking_enqueue_required);
    assert_eq!(
        event.auth_evidence[0].account_key_ref.as_deref(),
        Some("binance-prod")
    );
    assert!(!event.auth_evidence[0].trade_permission_confirmed);
}

#[test]
fn app_runtime_should_stop_new_opens_after_five_consecutive_losing_closes() {
    let captured_at = fixed_now();
    let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
        "risk": {
            "max_consecutive_losses": 5
        }
    }));
    let mut runtime = CrossArbAppRuntime::new(config).with_live_orders_enabled(true);

    for index in 1..=4 {
        let cycle = runtime.run_cycle(
            ready_input_with_loss(index, 1),
            captured_at + chrono::Duration::seconds(index),
        );
        assert_eq!(cycle.execution.submitted_intents, 1);
        assert_eq!(cycle.execution.consecutive_loss_closes, index as u32);
        assert!(!cycle.execution.stopped_by_loss_guard);
        assert!(cycle.execution.new_opens_allowed);
    }

    let stopped = runtime.run_cycle(
        ready_input_with_loss(5, 1),
        captured_at + chrono::Duration::seconds(5),
    );

    assert_eq!(stopped.execution.requested_intents, 1);
    assert_eq!(stopped.execution.submitted_intents, 0);
    assert_eq!(stopped.execution.consecutive_loss_closes, 5);
    assert_eq!(stopped.execution.max_consecutive_losses, 5);
    assert!(stopped.execution.blocked_by_loss_guard);
    assert!(stopped.execution.stopped_by_loss_guard);
    assert!(!stopped.execution.new_opens_allowed);
    assert!(runtime.stopped_by_loss_guard());
    assert!(stopped
        .notifications
        .iter()
        .any(|notification| notification.notification_kind == "risk_auto_stop"));
    assert!(stopped
        .storage_events
        .iter()
        .any(|event| event.event_kind == "risk_auto_stop"));
}

fn ready_exchanges() -> Vec<CrossArbExchangeReadinessRow> {
    ["binance", "gate", "bitget"]
        .into_iter()
        .map(|exchange| CrossArbExchangeReadinessRow {
            exchange: exchange.to_string(),
            expected_symbol_count: 1,
            market_data_streamed_symbols: 1,
            market_data_stream_ready: true,
            user_stream_ready: true,
            server_time_synced: true,
            precision_rules_ready: true,
            ready: true,
        })
        .collect()
}

fn ready_input_with_loss(index: i64, execution_intents: usize) -> CrossArbRuntimeInput {
    CrossArbRuntimeInput {
        execution_intents,
        exchange_readiness_rows: ready_exchanges(),
        db_writer_ready: true,
        position_takeover_complete: true,
        singleton_acquired: true,
        price_audit_events: vec![close_price_audit_event(
            &format!("loss-bundle-{index}"),
            "-0.01",
        )],
        ..Default::default()
    }
}

fn close_price_audit_event(bundle_id: &str, actual_pnl: &str) -> CrossArbDbPriceAuditEvent {
    let mut event = price_audit_event(bundle_id, "100.00", Some("99.99"));
    event.event_kind = "close_price_audit";
    event.lifecycle = "close";
    event.actual_pnl_usdt = Some(actual_pnl.to_string());
    event
}

fn price_audit_event(
    bundle_id: &str,
    planned_price: &str,
    actual_price: Option<&str>,
) -> CrossArbDbPriceAuditEvent {
    CrossArbDbPriceAuditEvent {
        event_kind: "open_price_audit",
        bundle_id: bundle_id.to_string(),
        lifecycle: "open",
        correlation_id: format!("{bundle_id}:open"),
        planned_at: fixed_now(),
        filled_at: Some(fixed_now()),
        recorded_at: fixed_now(),
        non_blocking_enqueue_required: false,
        legs: vec![CrossArbDbPriceAuditLeg {
            exchange: "binance".to_string(),
            symbol: "EDGE/USDT".to_string(),
            side: "buy".to_string(),
            position_side: "long".to_string(),
            planned_execution_price: planned_price.to_string(),
            actual_fill_price: actual_price.map(ToOwned::to_owned),
            planned_base_quantity: "0.055".to_string(),
            actual_base_quantity: Some("0.055".to_string()),
            planned_notional_usdt: "5.5".to_string(),
            actual_notional_usdt: Some("5.5011".to_string()),
            fee_usdt: Some("0.0022".to_string()),
            order_id: Some(format!("{bundle_id}-order")),
            client_order_id: Some(format!("{bundle_id}-client")),
        }],
        expected_net_pnl_usdt: Some("0.01".to_string()),
        actual_pnl_usdt: Some("0.008".to_string()),
        failure_reason: None,
    }
}

fn open_decision_audit_event() -> CrossArbDbOpenDecisionAuditEvent {
    let audit = OpenOpportunityAudit {
        opportunity_id: "EDGE/USDT:binance:gate:1780920000000".to_string(),
        canonical_symbol: CanonicalSymbol::new("EDGE", "USDT"),
        long_exchange: ExchangeId::new("binance"),
        short_exchange: ExchangeId::new("gate"),
        decision: OpenOpportunityDecision::Rejected,
        reject_reason: Some(OpenOpportunityRejectReason::SymbolAlreadyActive),
        raw_spread_pct: 0.012,
        configured_open_spread_pct: 0.005,
        min_open_spread_pct: 0.005,
        max_open_spread_pct: 0.05,
        min_open_net_profit_pct: 0.0,
        long_entry_price: 100.0,
        short_entry_price: 101.2,
        long_book_age_ms: 12,
        short_book_age_ms: 15,
        long_top_depth_usdt: 500.0,
        short_top_depth_usdt: 450.0,
        executable_top_depth_usdt: 360.0,
        target_notional_usdt: 50.0,
        quantity: Some(0.5),
        expected_net_profit_pct: Some(0.009),
        expected_net_pnl_usdt: Some(0.45),
        estimated_round_trip_fee_usdt: Some(0.1),
        observed_at: fixed_now(),
    };

    CrossArbDbOpenDecisionAuditEvent::from_open_audit(
        audit,
        "live",
        vec![CrossArbAuthEvidence {
            exchange: "binance".to_string(),
            account_id: Some("acct-main".to_string()),
            account_key_ref: Some("binance-prod".to_string()),
            auth_material_loaded: true,
            trade_permission_confirmed: false,
            private_stream_ready: true,
            evidence_reason: Some(
                "account key loaded but trade permission preflight failed".to_string(),
            ),
        }],
        fixed_now(),
    )
}

fn fixed_now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 8, 12, 0, 0)
        .single()
        .expect("valid fixed time")
}
