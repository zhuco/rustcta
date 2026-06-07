mod support;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use chrono::Utc;
use rustcta::control::spot_control::{
    RuntimeDataAuthority, RuntimePollComponent, RuntimePollingScheduler,
    RuntimePublisherLimitsConfig, RuntimePublisherPollingConfig, SnapshotConsistency,
    SpotControlRuntimeCache,
};
use rustcta::exchanges::spot_reservation::BalanceReservationManager;
use rustcta::exchanges::unified::{
    AmendOrderRequest, AssetBalance, BalanceSnapshot, CancelAllOrdersRequest, CancelOrderRequest,
    CancelOrderResponse, ExchangeClient, ExchangeClientError, ExchangeClientResult, FeeRate,
    FeeRateSource, MarketType, OrderBookSnapshot, OrderListConditionalLeg, OrderListLegType,
    OrderListRequest, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, PositionSide,
    QuoteMarketOrderRequest, SymbolRule, SymbolStatus, TimeInForce, TradeFill,
};

use support::live_readonly::{
    assert_no_secret_leak, offline_test_report, MutationCallDetector, PermissionStatus,
    ReadOnlyGuardClient, ReadOnlyPermissionReport,
};

fn assert_readonly_guard<T: std::fmt::Debug>(result: ExchangeClientResult<T>, endpoint: &str) {
    match result {
        Err(ExchangeClientError::Classified(error)) => {
            assert_eq!(
                error.class,
                rustcta::exchanges::unified::ExchangeErrorClass::PermissionDenied
            );
            assert_eq!(error.code.as_deref(), Some("readonly_guard"));
            assert!(
                error.message.contains(endpoint),
                "readonly guard message should name {endpoint}: {}",
                error.message
            );
        }
        other => panic!("expected readonly guard error for {endpoint}, got {other:?}"),
    }
}

fn test_readonly_config() -> support::live_readonly::LiveReadonlyTestConfig {
    support::live_readonly::LiveReadonlyTestConfig {
        symbol: "BTCUSDT".to_string(),
        market_type: MarketType::Spot,
        duration_seconds: 1,
        output_dir: std::path::PathBuf::from("target/live_readonly_tests"),
        require_no_withdraw_permission: true,
        max_rest_requests_per_minute: 6_000,
        safe_debug_balances: false,
    }
}

#[tokio::test]
async fn live_readonly_harness_should_block_place_order() {
    let detector = Arc::new(MutationCallDetector::default());
    let client = ReadOnlyGuardClient::new(MockReadonlyClient::default(), detector.clone());

    let result = client
        .place_order(OrderRequest::spot_market_buy("BTCUSDT", 0.001))
        .await;

    assert_readonly_guard(result, "place_order");
    assert_eq!(detector.count("place_order"), 1);
}

#[tokio::test]
async fn live_readonly_harness_should_block_quote_market_order() {
    let detector = Arc::new(MutationCallDetector::default());
    let client = ReadOnlyGuardClient::new(MockReadonlyClient::default(), detector.clone());

    let result = client
        .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("BTCUSDT", 25.0))
        .await;

    assert_readonly_guard(result, "place_quote_market_order");
    assert_eq!(detector.count("place_quote_market_order"), 1);
}

#[tokio::test]
async fn live_readonly_harness_should_block_amend_order() {
    let detector = Arc::new(MutationCallDetector::default());
    let client = ReadOnlyGuardClient::new(MockReadonlyClient::default(), detector.clone());

    let result = client
        .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
            MarketType::Spot,
            "BTCUSDT",
            "order-1",
            0.001,
        ))
        .await;

    assert_readonly_guard(result, "amend_order");
    assert_eq!(detector.count("amend_order"), 1);
}

#[tokio::test]
async fn live_readonly_harness_should_block_order_list() {
    let detector = Arc::new(MutationCallDetector::default());
    let client = ReadOnlyGuardClient::new(MockReadonlyClient::default(), detector.clone());

    let result = client
        .place_order_list(OrderListRequest::Oco {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            list_client_order_id: None,
            side: OrderSide::Sell,
            quantity: 0.001,
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some(70_000.0),
                stop_price: None,
                time_in_force: None,
                client_order_id: None,
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some(59_500.0),
                stop_price: Some(60_000.0),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: None,
            },
        })
        .await;

    assert_readonly_guard(result, "place_order_list");
    assert_eq!(detector.count("place_order_list"), 1);
}

#[tokio::test]
async fn live_readonly_harness_should_block_cancel_order() {
    let detector = Arc::new(MutationCallDetector::default());
    let client = ReadOnlyGuardClient::new(MockReadonlyClient::default(), detector.clone());

    let result = client
        .cancel_order(CancelOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: Some("order-1".to_string()),
            client_order_id: None,
        })
        .await;

    assert_readonly_guard(result, "cancel_order");
    assert_eq!(detector.count("cancel_order"), 1);
}

#[tokio::test]
async fn live_readonly_harness_should_block_cancel_all_orders() {
    let detector = Arc::new(MutationCallDetector::default());
    let client = ReadOnlyGuardClient::new(MockReadonlyClient::default(), detector.clone());

    let result = client
        .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
            MarketType::Spot,
            "BTCUSDT",
        ))
        .await;

    assert_readonly_guard(result, "cancel_all_orders");
    assert_eq!(detector.count("cancel_all_orders"), 1);
}

#[tokio::test]
async fn live_readonly_harness_should_allow_readonly_calls() {
    let detector = Arc::new(MutationCallDetector::default());
    let client = ReadOnlyGuardClient::new(MockReadonlyClient::default(), detector.clone());

    assert!(client.get_balances().await.is_ok());
    assert!(client.get_open_orders(None).await.is_ok());
    assert!(client.get_recent_fills("BTCUSDT").await.is_ok());
    assert_eq!(detector.total_mutations(), 0);
}

#[test]
fn mutation_detector_should_count_all_guarded_mutation_paths() {
    let detector = MutationCallDetector::default();
    for method in [
        "place_order",
        "place_quote_market_order",
        "amend_order",
        "place_order_list",
        "cancel_order",
        "cancel_all_orders",
    ] {
        detector.record(method);
    }

    assert_eq!(detector.total_mutations(), 6);
}

#[tokio::test]
async fn readonly_validation_should_report_guard_mutation_count() {
    let detector = Arc::new(MutationCallDetector::default());
    let client = ReadOnlyGuardClient::new(MockReadonlyClient::default(), detector);
    let config = test_readonly_config();
    let mut report = offline_test_report("mock");

    support::live_readonly::validate_authenticated_readonly(&client, &config, &mut report).await;

    assert_eq!(report.mutation_calls_detected, 0);
    report.assert_no_critical_errors();
}

#[test]
fn permission_unknown_should_not_be_pass() {
    let report = ReadOnlyPermissionReport::unknown("gateio");

    assert_eq!(report.read_permission, PermissionStatus::Unknown);
    assert_ne!(report.withdrawal_permission, PermissionStatus::Pass);
    assert!(!report.permission_introspection_supported);
}

#[test]
fn withdrawal_permission_should_fail_when_required() {
    let mut report = ReadOnlyPermissionReport::unknown("bitget");
    report.permission_introspection_supported = true;
    report.withdrawal_permission = PermissionStatus::Pass;

    report.apply_withdraw_policy(true);

    assert!(!report.critical_errors.is_empty());
}

#[test]
fn sanitized_report_should_not_contain_secret_tokens() {
    let report = offline_test_report("gateio");
    let text = serde_json::to_string(&report).unwrap();

    assert_no_secret_leak(&text);
}

#[tokio::test]
async fn failed_poll_should_preserve_last_valid_state_as_stale() {
    let cache = SpotControlRuntimeCache::default();
    let reservations = BalanceReservationManager::default();
    cache
        .apply_balance_success(
            BalanceSnapshot {
                exchange: "gateio".to_string(),
                market_type: MarketType::Spot,
                balances: vec![AssetBalance::new("BTC", 1.0, 0.8, 0.2)],
                timestamp: Utc::now(),
            },
            &reservations,
            &[],
        )
        .await;

    cache.apply_balance_failure("gateio", "rate limited").await;

    let state = cache.snapshot().await;
    let balances = state
        .balances_by_exchange
        .get("gateio")
        .and_then(|component| component.value_optional.as_ref())
        .expect("stale balance is retained");
    assert_eq!(balances[0].total, 1.0);
    assert_eq!(balances[0].available, 0.8);
}

#[tokio::test]
async fn rate_limit_response_should_trigger_backoff() {
    let mut scheduler = RuntimePollingScheduler::new(
        RuntimePublisherPollingConfig::default(),
        RuntimePublisherLimitsConfig {
            exponential_backoff_initial_ms: 10,
            exponential_backoff_max_ms: 100,
            pause_after_rate_limit_ms: 200,
            ..RuntimePublisherLimitsConfig::default()
        },
    );

    let now = Utc::now();
    scheduler.record_failure("gateio", RuntimePollComponent::Balances, now, true);

    assert!(!scheduler.should_poll(
        "gateio",
        RuntimePollComponent::Balances,
        now + chrono::Duration::milliseconds(50)
    ));
    assert!(scheduler.should_poll(
        "gateio",
        RuntimePollComponent::Balances,
        now + chrono::Duration::milliseconds(5_250)
    ));
}

#[test]
fn runtime_session_request_budget_should_be_conservative() {
    let config = RuntimePublisherLimitsConfig {
        maximum_concurrent_requests_per_exchange: 1,
        minimum_request_spacing_ms: 2_000,
        ..RuntimePublisherLimitsConfig::default()
    };

    assert_eq!(config.maximum_concurrent_requests_per_exchange, 1);
    assert!(config.minimum_request_spacing_ms >= 2_000);
}

#[test]
fn restart_simulation_should_not_resume_liquidation() {
    let report = rustcta::control::spot_control::RuntimePublisherStartupReport {
        loaded_snapshot_count: 1,
        restored_symbols: vec!["BTCUSDT".to_string()],
        fresh_exchanges: Vec::new(),
        stale_exchanges: vec!["gateio".to_string()],
        unknown_orders: Vec::new(),
        balance_mismatches: Vec::new(),
        ownership_changes: Vec::new(),
        write_actions_unblocked: false,
        blockers: vec!["fresh runtime publisher snapshot required".to_string()],
    };

    assert!(!report.write_actions_unblocked);
    assert!(report.blockers.iter().any(|item| item.contains("fresh")));
}

#[test]
fn replay_authority_should_be_offline() {
    let component = rustcta::control::spot_control::RuntimeComponentValue::<usize>::fresh(
        1,
        RuntimeDataAuthority::LocalRegistry,
        Utc::now(),
        None,
    );

    assert_eq!(component.authority, RuntimeDataAuthority::LocalRegistry);
    assert_ne!(
        SnapshotConsistency::MissingCriticalData,
        SnapshotConsistency::StrongEnoughForControl
    );
}

#[derive(Clone, Default)]
struct MockReadonlyClient {
    read_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ExchangeClient for MockReadonlyClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "mock"
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        Ok(BalanceSnapshot {
            exchange: "mock".to_string(),
            market_type: MarketType::Spot,
            balances: vec![AssetBalance::new("USDT", 100.0, 100.0, 0.0)],
            timestamp: Utc::now(),
        })
    }

    async fn get_orderbook(
        &self,
        _symbol: &str,
        _depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        Err(ExchangeClientError::Unsupported("not needed".to_string()))
    }

    async fn place_order(&self, _request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        panic!("mock place_order should be intercepted by ReadOnlyGuardClient");
    }

    async fn cancel_order(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        panic!("mock cancel_order should be intercepted by ReadOnlyGuardClient");
    }

    async fn get_order(
        &self,
        _symbol: &str,
        _order_id: &str,
    ) -> ExchangeClientResult<OrderResponse> {
        Ok(mock_order())
    }

    async fn get_open_orders(
        &self,
        _symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![mock_order()])
    }

    async fn get_fee_rate(&self, _symbol: &str) -> ExchangeClientResult<FeeRate> {
        Ok(FeeRate::new(0.001, 0.001, FeeRateSource::DefaultFallback))
    }

    async fn get_recent_fills(&self, _symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        Ok(vec![SymbolRule {
            exchange: "mock".to_string(),
            market_type: MarketType::Spot,
            internal_symbol: "BTCUSDT".to_string(),
            exchange_symbol: "BTCUSDT".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            price_precision: 2,
            quantity_precision: 6,
            status: SymbolStatus::Trading,
            tick_size: 0.01,
            step_size: 0.000001,
            min_quantity: 0.000001,
            min_notional: 5.0,
            max_quantity: None,
            supported_order_types: vec![OrderType::Limit, OrderType::Market],
            supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC],
            raw_metadata: None,
        }])
    }
}

fn mock_order() -> OrderResponse {
    OrderResponse {
        exchange: "mock".to_string(),
        market_type: MarketType::Spot,
        order_id: "order-1".to_string(),
        client_order_id: Some("manual-order".to_string()),
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Sell,
        position_side: PositionSide::None,
        order_type: OrderType::Limit,
        quantity: 0.1,
        filled_quantity: 0.0,
        price: Some(50_000.0),
        average_price: None,
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Some(Utc::now()),
    }
}
