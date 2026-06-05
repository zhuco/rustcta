use tempfile::tempdir;

use crate::exchanges::unified::{
    AssetBalance, BalanceSnapshot, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot,
    OrderResponse, OrderSide, OrderStatus, OrderType, PositionSide, SymbolRule, SymbolStatus,
    TimeInForce, TradeFill,
};
use crate::execution::{FeeConfig, FeeModel, FeePairConfig};
use crate::risk::{
    DisabledConfig, DisabledExchangeSymbol, DisabledRegistry, DisabledRegistryConfig,
    UnmanagedPosition,
};

use super::*;

fn ready_snapshot() -> EnableValidationSnapshot {
    EnableValidationSnapshot {
        kill_switch_allows_paper: true,
        kill_switch_allows_live_dry_run: true,
        kill_switch_allows_live_orders: false,
        live_preflight_allows_live_dry_run: true,
        live_preflight_allows_live_orders: false,
        small_live_gate_allows: false,
        ..EnableValidationSnapshot::default()
    }
    .mark_exchange_ready("gateio", "AURORAUSDT")
    .mark_exchange_ready("bitget", "AURORAUSDT")
}

fn enable_request(expected_version: u64) -> EnableSymbolRequest {
    EnableSymbolRequest {
        symbol: "AURORAUSDT".to_string(),
        mode: EnableMode::LiveDryRun,
        selected_exchanges: vec!["gateio".to_string(), "bitget".to_string()],
        allowed_directions: vec![
            EnabledDirection {
                buy_exchange: "gateio".to_string(),
                sell_exchange: "bitget".to_string(),
            },
            EnabledDirection {
                buy_exchange: "bitget".to_string(),
                sell_exchange: "gateio".to_string(),
            },
        ],
        max_notional_per_trade: Some(3.0),
        max_total_symbol_exposure: Some(10.0),
        quote_asset: "USDT".to_string(),
        requested_by: "operator".to_string(),
        expected_version,
    }
}

fn runtime_rule(exchange: &str) -> SymbolRule {
    SymbolRule {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        internal_symbol: "AURORAUSDT".to_string(),
        exchange_symbol: "AURORAUSDT".to_string(),
        base_asset: "AURORA".to_string(),
        quote_asset: "USDT".to_string(),
        price_precision: 4,
        quantity_precision: 4,
        tick_size: 0.0001,
        step_size: 0.0001,
        min_quantity: 0.1,
        min_notional: 1.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Limit, OrderType::IOC, OrderType::PostOnly],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::GTX],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

fn runtime_book(exchange: &str, stale: bool) -> OrderBookSnapshot {
    OrderBookSnapshot {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        symbol: "AURORAUSDT".to_string(),
        bids: vec![
            OrderBookLevel {
                price: 1.0,
                quantity: 5.0,
            },
            OrderBookLevel {
                price: 0.99,
                quantity: 10.0,
            },
        ],
        asks: vec![OrderBookLevel {
            price: 1.01,
            quantity: 10.0,
        }],
        best_bid: Some(1.0),
        best_ask: Some(1.01),
        exchange_timestamp: Some(chrono::Utc::now()),
        received_at: if stale {
            chrono::Utc::now() - chrono::Duration::milliseconds(5_000)
        } else {
            chrono::Utc::now()
        },
        latency_ms: Some(1),
        sequence: Some(1),
        is_stale: stale,
    }
}

fn runtime_balance(
    exchange: &str,
    asset: &str,
    total: f64,
    available: f64,
    locked: f64,
    reserved: f64,
) -> RuntimeBalanceView {
    RuntimeBalanceView {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        asset: asset.to_string(),
        total,
        available,
        locked_by_exchange: locked,
        locally_reserved: reserved,
        effective_available: (available - reserved).max(0.0),
        source: "test".to_string(),
        updated_at: Some(chrono::Utc::now()),
    }
}

fn runtime_order(
    client_order_id: Option<&str>,
    side: OrderSide,
    quantity: f64,
    filled_quantity: f64,
) -> OrderResponse {
    OrderResponse {
        exchange: "gateio".to_string(),
        market_type: MarketType::Spot,
        symbol: "AURORAUSDT".to_string(),
        order_id: format!("order-{}", client_order_id.unwrap_or("manual")),
        client_order_id: client_order_id.map(ToOwned::to_owned),
        side,
        position_side: PositionSide::None,
        order_type: OrderType::Limit,
        status: OrderStatus::New,
        price: Some(1.0),
        quantity,
        filled_quantity,
        average_price: None,
        created_at: chrono::Utc::now(),
        updated_at: Some(chrono::Utc::now()),
    }
}

fn runtime_fee_model() -> FeeModel {
    FeeModel::from_config(FeeConfig {
        defaults: std::collections::HashMap::from([
            (
                "gateio".to_string(),
                std::collections::HashMap::from([(
                    "spot".to_string(),
                    FeePairConfig {
                        maker_bps: 10.0,
                        taker_bps: 10.0,
                        fee_asset: Some("quote".to_string()),
                    },
                )]),
            ),
            (
                "bitget".to_string(),
                std::collections::HashMap::from([(
                    "spot".to_string(),
                    FeePairConfig {
                        maker_bps: 10.0,
                        taker_bps: 10.0,
                        fee_asset: Some("quote".to_string()),
                    },
                )]),
            ),
        ]),
        ..FeeConfig::default()
    })
}

fn runtime_inputs(stale_book: bool) -> SpotControlSnapshotInputs {
    SpotControlSnapshotInputs {
        symbol_rules: vec![runtime_rule("gateio"), runtime_rule("bitget")],
        books: vec![
            runtime_book("gateio", stale_book),
            runtime_book("bitget", false),
        ],
        fee_model: Some(runtime_fee_model()),
        balances: vec![
            runtime_balance("gateio", "USDT", 100.0, 100.0, 0.0, 0.0),
            runtime_balance("gateio", "AURORA", 10.0, 10.0, 1.0, 2.0),
            runtime_balance("bitget", "USDT", 100.0, 100.0, 0.0, 0.0),
            runtime_balance("bitget", "AURORA", 10.0, 10.0, 0.0, 0.0),
        ],
        disabled_registry: Some(DisabledRegistry::new()),
        kill_switch_state: Some(crate::risk::KillSwitchState {
            enabled: true,
            active: false,
            reason: None,
            triggered_by: None,
            triggered_at: None,
            allow_paper_trading: true,
            allow_live_dry_run: true,
            allow_live_orders: false,
        }),
        live_preflight_state: Some(crate::live_preflight::LivePreflightReport {
            timestamp: chrono::Utc::now(),
            decision: crate::live_preflight::LiveReadinessDecision::ReadyForLiveDryRun,
            target_mode: "live_dry_run".to_string(),
            checks: Vec::new(),
            pass_count: 1,
            warn_count: 0,
            fail_count: 0,
            skipped_count: 0,
            unknown_count: 0,
            critical_failures: Vec::new(),
            warnings: Vec::new(),
            suggested_next_actions: Vec::new(),
            per_exchange_readiness: std::collections::HashMap::new(),
            per_symbol_readiness: std::collections::HashMap::new(),
            config_summary: crate::live_preflight::LivePreflightConfig::default(),
        }),
        operation_lock_allows: true,
        balance_reconciliation_state: Some(crate::execution::BalanceReconciliationReport {
            timestamp: chrono::Utc::now(),
            statuses: Vec::new(),
            max_severity: crate::execution::BalanceMismatchSeverity::Info,
            clean: true,
        }),
        ..SpotControlSnapshotInputs::default()
    }
}

fn balance_snapshot(exchange: &str, balances: Vec<AssetBalance>) -> BalanceSnapshot {
    BalanceSnapshot {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        balances,
        timestamp: chrono::Utc::now(),
    }
}

#[test]
fn lifecycle_should_validate_expected_transitions() {
    assert!(validate_transition(
        SpotSymbolLifecycleState::Disabled,
        SpotSymbolLifecycleState::EnableRequested
    )
    .is_ok());
    assert!(validate_transition(
        SpotSymbolLifecycleState::Frozen,
        SpotSymbolLifecycleState::Active
    )
    .is_err());
}

#[tokio::test]
async fn enable_should_require_explicit_exchanges() {
    let service = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        ..SpotSymbolControlConfig::default()
    });
    service.set_validation_snapshot(ready_snapshot()).await;
    let mut request = enable_request(0);
    request.selected_exchanges.clear();
    let response = service
        .enable(request, "idem-empty-exchange".to_string())
        .await;
    assert_eq!(response.status, CommandStatus::Failed);
    assert!(response
        .validation_errors
        .iter()
        .any(|error| error.code == "selected_exchanges_required"));
}

#[tokio::test]
async fn enable_should_reject_missing_exchange_symbol() {
    let service = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        ..SpotSymbolControlConfig::default()
    });
    service
        .set_validation_snapshot(EnableValidationSnapshot {
            kill_switch_allows_live_dry_run: true,
            live_preflight_allows_live_dry_run: true,
            ..EnableValidationSnapshot::default()
        })
        .await;
    let response = service
        .enable(enable_request(0), "idem-missing".to_string())
        .await;
    assert_eq!(response.status, CommandStatus::Failed);
    assert!(response
        .validation_errors
        .iter()
        .any(|error| error.code == "exchange_symbol_missing"));
}

#[test]
fn runtime_polling_scheduler_should_rate_limit_backoff_per_exchange() {
    let mut scheduler = RuntimePollingScheduler::new(
        RuntimePublisherPollingConfig::default(),
        RuntimePublisherLimitsConfig::default(),
    );
    let now = chrono::Utc::now();
    assert!(scheduler.should_poll("gateio", RuntimePollComponent::Balances, now));
    scheduler.record_poll("gateio", RuntimePollComponent::Balances, now);
    assert!(!scheduler.should_poll(
        "gateio",
        RuntimePollComponent::Balances,
        now + chrono::Duration::milliseconds(1)
    ));
    scheduler.record_failure(
        "gateio",
        RuntimePollComponent::Balances,
        now + chrono::Duration::milliseconds(2),
        true,
    );
    assert!(!scheduler.should_poll(
        "gateio",
        RuntimePollComponent::OpenOrders,
        now + chrono::Duration::milliseconds(3)
    ));
    assert!(scheduler.should_poll(
        "bitget",
        RuntimePollComponent::Balances,
        now + chrono::Duration::milliseconds(3)
    ));
}

#[tokio::test]
async fn runtime_cache_failed_balance_poll_preserves_last_valid_stale_state() {
    let cache = SpotControlRuntimeCache::default();
    let reservations = crate::exchanges::spot_reservation::BalanceReservationManager::default();
    cache
        .apply_balance_success(
            balance_snapshot("gateio", vec![AssetBalance::new("AURORA", 10.0, 9.0, 1.0)]),
            &reservations,
            &[],
        )
        .await;
    cache
        .apply_balance_failure("gateio", "temporary REST failure")
        .await;

    let state = cache.snapshot().await;
    let component = state.balances_by_exchange.get("gateio").unwrap();
    assert_eq!(component.freshness_status, RuntimeComponentFreshness::Stale);
    let balances = component.value_optional.as_ref().unwrap();
    assert_eq!(balances[0].total, 10.0);
    assert_ne!(balances[0].total, 0.0);
}

#[tokio::test]
async fn runtime_cache_missing_asset_row_preserves_last_known_balance_without_zeroing() {
    let cache = SpotControlRuntimeCache::default();
    let reservations = crate::exchanges::spot_reservation::BalanceReservationManager::default();
    cache
        .apply_balance_success(
            balance_snapshot(
                "gateio",
                vec![
                    AssetBalance::new("AURORA", 10.0, 10.0, 0.0),
                    AssetBalance::new("USDT", 100.0, 100.0, 0.0),
                ],
            ),
            &reservations,
            &[],
        )
        .await;
    cache
        .apply_balance_success(
            balance_snapshot("gateio", vec![AssetBalance::new("USDT", 90.0, 90.0, 0.0)]),
            &reservations,
            &[],
        )
        .await;

    let state = cache.snapshot().await;
    let balances = state
        .balances_by_exchange
        .get("gateio")
        .unwrap()
        .value_optional
        .as_ref()
        .unwrap();
    let aurora = balances.iter().find(|item| item.asset == "AURORA").unwrap();
    assert_eq!(aurora.total, 10.0);
    assert_eq!(aurora.source, "last_valid_missing_from_exchange_response");
}

#[tokio::test]
async fn runtime_cache_open_orders_classifies_ownership_and_unknown_sell_blocks_inventory() {
    let cache = SpotControlRuntimeCache::default();
    cache
        .apply_open_orders_success(
            "gateio",
            vec![
                runtime_order(Some("spotctl-disable-1"), OrderSide::Sell, 1.0, 0.0),
                runtime_order(None, OrderSide::Sell, 2.0, 0.5),
            ],
        )
        .await;
    let inputs = cache
        .inputs_for_snapshot(&["gateio".to_string()])
        .await
        .open_orders;
    assert!(inputs
        .iter()
        .any(|order| order.ownership_class == OrderOwnershipClass::SpotControl));
    assert!(inputs.iter().any(|order| {
        order.ownership_class == OrderOwnershipClass::ManualOperator && !order.ownership_known
    }));

    let mut snapshot_inputs = runtime_inputs(false);
    snapshot_inputs.open_orders = inputs;
    let snapshot =
        SpotControlSnapshotBuilder::new(SpotControlSnapshotConfig::default(), snapshot_inputs)
            .build(
                "AURORAUSDT",
                &["gateio".to_string(), "bitget".to_string()],
                &enable_request(0).allowed_directions,
                EnableMode::LiveDryRun,
            );
    let gateio_base = snapshot
        .inventory_ownership
        .iter()
        .find(|item| item.exchange == "gateio" && item.asset == "AURORA")
        .unwrap();
    assert!(!gateio_base.ownership_known);
    assert!(gateio_base.effective_sellable_managed_quantity <= 6.5);
    assert!(snapshot.liquidation_preview.rejected);
}

#[tokio::test]
async fn runtime_cache_recent_fills_deduplicates_and_preserves_unknown_ownership() {
    let cache = SpotControlRuntimeCache::default();
    let fill = TradeFill {
        exchange: "gateio".to_string(),
        market_type: MarketType::Spot,
        symbol: "AURORAUSDT".to_string(),
        trade_id: Some("fill-1".to_string()),
        order_id: Some("order-1".to_string()),
        client_order_id: Some("unknown-prefix".to_string()),
        side: OrderSide::Buy,
        price: 1.0,
        quantity: 2.0,
        fee_asset: Some("USDT".to_string()),
        fee_amount: Some(0.01),
        liquidity: LiquidityRole::Taker,
        timestamp: chrono::Utc::now(),
    };
    cache
        .apply_fills_success("gateio", vec![fill.clone(), fill])
        .await;
    let fills = cache
        .fill_records_for_exchanges(&["gateio".to_string()])
        .await;
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].ownership_class, OrderOwnershipClass::Unknown);
}

#[test]
fn runtime_consistency_reports_strong_and_inconsistent_states() {
    let mut snapshot = SpotControlSnapshotBuilder::new(
        SpotControlSnapshotConfig::default(),
        runtime_inputs(false),
    )
    .build(
        "AURORAUSDT",
        &["gateio".to_string(), "bitget".to_string()],
        &enable_request(0).allowed_directions,
        EnableMode::LiveDryRun,
    );
    snapshot.critical_errors.clear();
    for status in &mut snapshot.component_statuses {
        status.status = RuntimeComponentFreshness::Fresh;
        status.warning_optional = None;
        status.updated_at = Some(snapshot.generated_at);
        status.age_ms = Some(0);
    }
    let report = SnapshotConsistencyReport::from_snapshot(&snapshot, 10_000);
    assert_eq!(report.status, SnapshotConsistency::StrongEnoughForControl);

    snapshot.component_statuses[0].updated_at =
        Some(snapshot.generated_at - chrono::Duration::milliseconds(20_000));
    let report = SnapshotConsistencyReport::from_snapshot(&snapshot, 10_000);
    assert_eq!(report.status, SnapshotConsistency::Inconsistent);
}

#[tokio::test]
async fn snapshot_store_persists_retrieves_immutably_and_replays_without_network() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("snapshots.jsonl");
    let store = std::sync::Arc::new(JsonlSpotControlSnapshotStore::new(path));
    let mut snapshot = SpotControlSnapshotBuilder::new(
        SpotControlSnapshotConfig::default(),
        runtime_inputs(false),
    )
    .build(
        "AURORAUSDT",
        &["gateio".to_string(), "bitget".to_string()],
        &enable_request(0).allowed_directions,
        EnableMode::LiveDryRun,
    );
    snapshot.source_metadata.push(RuntimeComponentMetadata {
        component: "balances:gateio".to_string(),
        authority: RuntimeDataAuthority::ExchangeRest,
        fetched_at: chrono::Utc::now(),
        exchange_timestamp_optional: None,
        age_ms: 0,
        freshness_status: RuntimeComponentFreshness::Fresh,
        error_optional: None,
    });
    store.persist_snapshot(&snapshot).await.unwrap();
    assert!(store.persist_snapshot(&snapshot).await.is_err());
    let loaded = store
        .get_snapshot(&snapshot.snapshot_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.snapshot_id, snapshot.snapshot_id);
    let replay = SpotControlSnapshotReplay::new(store)
        .replay(&snapshot.snapshot_id)
        .await
        .unwrap()
        .unwrap();
    assert!(replay.matching);
}

#[tokio::test]
async fn disabled_to_enable_requested_to_active_should_persist() {
    let dir = tempdir().unwrap();
    let config = SpotSymbolControlConfig {
        enabled: true,
        command_store_path: dir
            .path()
            .join("commands.jsonl")
            .to_string_lossy()
            .to_string(),
        audit_store_path: dir.path().join("audit.jsonl").to_string_lossy().to_string(),
        lifecycle_store_path: dir
            .path()
            .join("symbols.jsonl")
            .to_string_lossy()
            .to_string(),
        ..SpotSymbolControlConfig::default()
    };
    let service = SpotControlService::load_or_new(config.clone()).unwrap();
    service.set_validation_snapshot(ready_snapshot()).await;
    let response = service
        .enable(enable_request(0), "idem-enable".to_string())
        .await;
    assert_eq!(response.status, CommandStatus::Completed);
    assert_eq!(
        response.lifecycle_state,
        Some(SpotSymbolLifecycleState::Active)
    );

    let reloaded = SpotControlService::load_or_new(config).unwrap();
    let symbol = reloaded.symbol("AURORAUSDT").await.unwrap();
    assert_eq!(symbol.lifecycle_state, SpotSymbolLifecycleState::Active);
    assert_eq!(symbol.version, 1);
}

#[tokio::test]
async fn duplicate_idempotency_key_should_return_previous_command() {
    let service = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        ..SpotSymbolControlConfig::default()
    });
    service.set_validation_snapshot(ready_snapshot()).await;
    let first = service
        .enable(enable_request(0), "same-key".to_string())
        .await;
    let second = service
        .enable(enable_request(0), "same-key".to_string())
        .await;
    assert_eq!(first.command_id, second.command_id);
    assert_eq!(first.current_version, second.current_version);
}

#[tokio::test]
async fn stale_expected_version_should_reject_update() {
    let service = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        ..SpotSymbolControlConfig::default()
    });
    service.set_validation_snapshot(ready_snapshot()).await;
    let _ = service
        .enable(enable_request(0), "idem-v1".to_string())
        .await;
    let response = service
        .enable(enable_request(0), "idem-stale".to_string())
        .await;
    assert_eq!(response.status, CommandStatus::Rejected);
    assert!(response
        .validation_errors
        .iter()
        .any(|error| error.code == "stale_lifecycle_version"));
}

#[tokio::test]
async fn freeze_only_should_block_without_selling_inventory() {
    let service = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        ..SpotSymbolControlConfig::default()
    });
    service.set_validation_snapshot(ready_snapshot()).await;
    let _ = service
        .enable(enable_request(0), "idem-enable-freeze".to_string())
        .await;
    let response = service
        .disable(
            DisableSymbolRequest {
                symbol: "AURORAUSDT".to_string(),
                selected_exchanges: vec!["gateio".to_string(), "bitget".to_string()],
                mode: DisableMode::FreezeOnly,
                cancel_active_orders: true,
                include_managed_inventory_only: true,
                maximum_liquidation_loss_usdt: Some(2.0),
                maximum_slippage_bps: Some(30.0),
                requested_by: "operator".to_string(),
                expected_version: 1,
            },
            "idem-freeze".to_string(),
        )
        .await;
    assert_eq!(
        response.lifecycle_state,
        Some(SpotSymbolLifecycleState::DisabledWithInventory)
    );
    assert_eq!(
        response
            .result_summary
            .as_ref()
            .and_then(|value| value.get("sold_inventory"))
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
}

#[tokio::test]
async fn market_liquidate_should_create_ioc_limit_dry_run_plan_only() {
    let service = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        ..SpotSymbolControlConfig::default()
    });
    service.set_validation_snapshot(ready_snapshot()).await;
    let _ = service
        .enable(enable_request(0), "idem-enable-market".to_string())
        .await;
    let response = service
        .disable(
            DisableSymbolRequest {
                symbol: "AURORAUSDT".to_string(),
                selected_exchanges: vec!["gateio".to_string()],
                mode: DisableMode::MarketLiquidate,
                cancel_active_orders: true,
                include_managed_inventory_only: true,
                maximum_liquidation_loss_usdt: Some(2.0),
                maximum_slippage_bps: Some(30.0),
                requested_by: "operator".to_string(),
                expected_version: 1,
            },
            "idem-market".to_string(),
        )
        .await;
    assert_eq!(response.status, CommandStatus::AwaitingConfirmation);
    assert_eq!(
        response
            .result_summary
            .as_ref()
            .and_then(|value| value.get("order_type"))
            .and_then(serde_json::Value::as_str),
        Some("IOC limit sell")
    );
    let model = service.read_model().await;
    assert_eq!(model.liquidation_plans.len(), 1);
    assert!(!model.liquidation_plans[0].would_submit_order);
}

#[tokio::test]
async fn operation_lock_should_block_conflicting_lifecycle_owner() {
    let mut locks = SymbolOperationLockRegistry::default();
    let first = locks.acquire(
        "AURORAUSDT",
        SymbolOperationOwner::DisableWorkflow,
        "cmd-1",
        1_000,
        chrono::Utc::now(),
    );
    assert!(first.is_ok());
    let second = locks.acquire(
        "AURORAUSDT",
        SymbolOperationOwner::ManualOperation,
        "cmd-2",
        1_000,
        chrono::Utc::now(),
    );
    assert!(second.is_err());
    assert!(!locks.operation_allows_arbitrage("AURORAUSDT"));
}

#[test]
fn runtime_snapshot_builder_should_build_from_authoritative_inputs() {
    let builder = SpotControlSnapshotBuilder::new(
        SpotControlSnapshotConfig {
            require_exchange_health: false,
            ..SpotControlSnapshotConfig::default()
        },
        runtime_inputs(false),
    );
    let snapshot = builder.build(
        "AURORAUSDT",
        &["gateio".to_string(), "bitget".to_string()],
        &enable_request(0).allowed_directions,
        EnableMode::LiveDryRun,
    );
    assert!(
        snapshot.critical_errors.is_empty(),
        "{:?}",
        snapshot.critical_errors
    );
    assert_eq!(snapshot.symbol_rules.len(), 2);
    assert_eq!(snapshot.books.len(), 2);
    assert_eq!(snapshot.fees.len(), 2);
    assert!(snapshot
        .component_statuses
        .iter()
        .any(|status| status.component == "book:gateio:AURORAUSDT"
            && status.status == RuntimeComponentFreshness::Fresh));
}

#[test]
fn runtime_snapshot_should_mark_missing_and_stale_book() {
    let mut inputs = runtime_inputs(true);
    inputs.books.retain(|book| book.exchange != "bitget");
    let builder = SpotControlSnapshotBuilder::new(
        SpotControlSnapshotConfig {
            require_exchange_health: false,
            ..SpotControlSnapshotConfig::default()
        },
        inputs,
    );
    let snapshot = builder.build(
        "AURORAUSDT",
        &["gateio".to_string(), "bitget".to_string()],
        &enable_request(0).allowed_directions,
        EnableMode::LiveDryRun,
    );
    assert!(snapshot
        .critical_errors
        .iter()
        .any(|error| error.contains("stale book for gateio:AURORAUSDT")));
    assert!(snapshot
        .critical_errors
        .iter()
        .any(|error| error.contains("missing book for bitget:AURORAUSDT")));
}

#[test]
fn inventory_ownership_should_exclude_unmanaged_reserved_and_locked() {
    let mut inputs = runtime_inputs(false);
    inputs.unmanaged_positions.push(UnmanagedPosition {
        exchange: "gateio".to_string(),
        market_type: MarketType::Spot,
        symbol: "AURORAUSDT".to_string(),
        asset: "AURORA".to_string(),
        quantity: 3.0,
        reason: "manual".to_string(),
        created_at: chrono::Utc::now(),
    });
    inputs
        .other_strategy_inventory
        .push(OtherStrategyInventory {
            exchange: "gateio".to_string(),
            symbol: "AURORAUSDT".to_string(),
            asset: "AURORA".to_string(),
            quantity: 1.0,
            owning_strategy: "other".to_string(),
        });
    let snapshot = SpotControlSnapshotBuilder::new(
        SpotControlSnapshotConfig {
            require_exchange_health: false,
            ..SpotControlSnapshotConfig::default()
        },
        inputs,
    )
    .build(
        "AURORAUSDT",
        &["gateio".to_string()],
        &[],
        EnableMode::LiveDryRun,
    );
    let ownership = snapshot
        .inventory_ownership
        .iter()
        .find(|item| item.exchange == "gateio" && item.asset == "AURORA")
        .unwrap();
    assert_eq!(ownership.spot_control_managed_quantity, 6.0);
    assert_eq!(ownership.effective_sellable_managed_quantity, 3.0);
}

#[test]
fn disabled_registry_should_override_runtime_snapshot() {
    let mut inputs = runtime_inputs(false);
    inputs.disabled_registry = Some(DisabledRegistry::from_config(DisabledRegistryConfig {
        disabled: DisabledConfig {
            exchange_symbols: vec![DisabledExchangeSymbol {
                exchange: "gateio".to_string(),
                market_type: MarketType::Spot,
                symbol: "AURORAUSDT".to_string(),
                reason: "risk".to_string(),
                expires_at: None,
            }],
            ..DisabledConfig::default()
        },
        unmanaged_positions: Vec::new(),
    }));
    let snapshot = SpotControlSnapshotBuilder::new(
        SpotControlSnapshotConfig {
            require_exchange_health: false,
            ..SpotControlSnapshotConfig::default()
        },
        inputs,
    )
    .build(
        "AURORAUSDT",
        &["gateio".to_string()],
        &[],
        EnableMode::LiveDryRun,
    );
    assert!(snapshot
        .critical_errors
        .iter()
        .any(|error| error.contains("DisabledRegistry blocks gateio:AURORAUSDT")));
}

#[test]
fn liquidation_preview_should_use_managed_sellable_inventory_only() {
    let snapshot = SpotControlSnapshotBuilder::new(
        SpotControlSnapshotConfig {
            require_exchange_health: false,
            ..SpotControlSnapshotConfig::default()
        },
        runtime_inputs(false),
    )
    .build(
        "AURORAUSDT",
        &["gateio".to_string()],
        &[],
        EnableMode::LiveDryRun,
    );
    let plan = snapshot.liquidation_preview.market_plans.first().unwrap();
    assert_eq!(plan.managed_sellable_quantity, 7.0);
    assert!(plan.executable_vwap > 0.0);
    assert!(plan.estimated_fee > 0.0);
    assert!(!plan.would_submit_order);
}

#[tokio::test]
async fn command_should_store_snapshot_id() {
    let service = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        ..SpotSymbolControlConfig::default()
    });
    let snapshot = SpotControlSnapshotBuilder::new(
        SpotControlSnapshotConfig {
            require_exchange_health: false,
            ..SpotControlSnapshotConfig::default()
        },
        runtime_inputs(false),
    )
    .build(
        "AURORAUSDT",
        &["gateio".to_string(), "bitget".to_string()],
        &enable_request(0).allowed_directions,
        EnableMode::LiveDryRun,
    );
    let snapshot_id = snapshot.snapshot_id.clone();
    service.record_runtime_snapshot(snapshot).await;
    let response = service
        .enable(enable_request(0), "idem-runtime-snapshot".to_string())
        .await;
    assert_eq!(response.status, CommandStatus::Completed);
    let model = service.read_model().await;
    assert_eq!(
        model.commands[0].snapshot_id.as_deref(),
        Some(snapshot_id.as_str())
    );
}
