use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use chrono::Utc;
use serde_json::Value;
use tower::ServiceExt;

use crate::control::spot_control::{
    EnableValidationSnapshot, SpotControlService, SpotSymbolControlConfig,
};
use crate::exchanges::unified::{MarketType, OrderType, SymbolRule, SymbolStatus, TimeInForce};
use crate::execution::FeeSource;
use crate::live_preflight::{
    build_report, LivePreflightCheck, LivePreflightCheckStatus, LivePreflightConfig,
    LivePreflightGate, LivePreflightSeverity,
};
use crate::strategies::arbitrage_core::{
    AccountStructure, ArbitrageOpportunityAnalysis, ArbitrageRelationshipType,
    ArbitrageStatisticsSnapshot, ConfidenceLevel, RiskScoreComponents,
};

use super::*;

fn test_state(require_token: bool) -> MonitoringState {
    let config = MonitoringConfig {
        require_token,
        token_env: "RUSTCTA_MONITOR_TEST_TOKEN".to_string(),
        ..MonitoringConfig::default()
    };
    let mut model = DashboardReadModel::default();
    model.strategy.strategy_name = "spot_spot_taker_arbitrage".to_string();
    model.exchanges.push(ExchangeHealthView {
        exchange: "mexc".to_string(),
        market_type: Some(MarketType::Spot),
        connected: true,
        public_ws_connected: true,
        private_ws_connected: None,
        last_message_at: Some(Utc::now()),
        last_book_update_at: Some(Utc::now()),
        stale_symbol_count: 0,
        fresh_symbol_count: 1,
        reconnect_count: 0,
        parse_error_count: 0,
        sequence_gap_count: 0,
        heartbeat_timeout_count: 0,
        avg_latency_ms: Some(2.0),
        max_latency_ms: Some(3),
    });
    model.books.push(BookView {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        exchange_symbol: "BTCUSDT".to_string(),
        best_bid: Some(99.0),
        best_ask: Some(100.0),
        spread: Some(1.0),
        book_age_ms: 10,
        latency_ms: Some(1),
        is_stale: false,
        stale_reason: None,
        source: "websocket".to_string(),
        sequence: Some(1),
    });
    model.books.push(BookView {
        exchange: "coinex".to_string(),
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        exchange_symbol: "BTCUSDT".to_string(),
        best_bid: Some(98.0),
        best_ask: Some(101.0),
        spread: Some(3.0),
        book_age_ms: 2_000,
        latency_ms: Some(5),
        is_stale: true,
        stale_reason: Some("heartbeat_timeout".to_string()),
        source: "websocket".to_string(),
        sequence: Some(2),
    });
    model.opportunities.push(OpportunityView {
        timestamp: Utc::now(),
        opportunity_id: "opp-1".to_string(),
        symbol: "BTCUSDT".to_string(),
        relationship_type: "spot_spot_taker".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_price: 100.0,
        sell_price: 101.0,
        raw_spread_bps: 100.0,
        fee_bps: 25.0,
        net_spread_bps: 75.0,
        estimated_net_pnl: 7.5,
        accepted: true,
        rejection_reason: None,
        buy_book_age_ms: 10,
        sell_book_age_ms: 11,
    });
    model.trades.push(TradeView {
        timestamp: Utc::now(),
        trade_id: "trade-1".to_string(),
        symbol: "BTCUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        quantity: 0.1,
        buy_avg_price: 100.0,
        sell_avg_price: 101.0,
        gross_pnl: 1.0,
        total_fee: 0.2,
        net_pnl: 0.8,
        execution_mode: "paper_taker_taker".to_string(),
        paper_or_live: "paper".to_string(),
    });
    model.disabled.symbols.push(DisabledSymbolView {
        symbol: "TURBOSUSDT".to_string(),
        status: "active".to_string(),
        reason: "manual".to_string(),
        expires_at: None,
    });
    model.unmanaged_positions.push(UnmanagedPositionView {
        exchange: "coinex".to_string(),
        market_type: MarketType::Spot,
        symbol: "PONDUSDT".to_string(),
        asset: "POND".to_string(),
        quantity: 5_000.0,
        reason: "legacy".to_string(),
        created_at: Utc::now(),
    });
    model.fees.push(FeeView {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        symbol: None,
        maker_fee_bps: 0.0,
        taker_fee_bps: 5.0,
        source: FeeSource::ConfigDefault,
        platform_discount_enabled: false,
        platform_token: None,
        updated_at: Utc::now(),
    });
    model.config_summary = ConfigSummaryView {
        enabled_exchanges: vec!["mexc".to_string(), "coinex".to_string()],
        enabled_symbols: vec!["BTCUSDT".to_string()],
        trading_mode: "paper".to_string(),
        live_trading_enabled: false,
        dry_run: true,
        max_notional_per_trade: Some(100.0),
        max_notional_per_symbol: Some(500.0),
        max_total_notional: Some(1_000.0),
        fee_config_summary: Some("config/fees.yml".to_string()),
        disabled_config_summary: Some("config/disabled_symbols.yml".to_string()),
        secrets_redacted: true,
    };
    model.arbitrage_opportunities.push(arbitrage_analysis());
    model
        .arbitrage_statistics
        .push(ArbitrageStatisticsSnapshot {
            spread_duration_samples: 150,
            confidence: ConfidenceLevel::Medium,
            ..ArbitrageStatisticsSnapshot::default()
        });
    model.live_preflight_enabled = true;
    model.live_preflight = Some(build_report(
        LivePreflightConfig {
            enabled: true,
            max_live_notional_per_trade: Some(5.0),
            max_total_live_notional: Some(20.0),
            ..LivePreflightConfig::default()
        },
        vec![LivePreflightCheck::new(
            LivePreflightGate::ConfigSafety,
            "test_check",
            LivePreflightCheckStatus::Pass,
            LivePreflightSeverity::Info,
            "ok",
        )],
    ));
    MonitoringState::from_read_model(config, model)
}

async fn control_state(require_token: bool) -> MonitoringState {
    let state = test_state(require_token);
    let service = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        ..SpotSymbolControlConfig::default()
    });
    service
        .set_validation_snapshot(
            EnableValidationSnapshot {
                kill_switch_allows_paper: true,
                kill_switch_allows_live_dry_run: true,
                live_preflight_allows_live_dry_run: true,
                ..EnableValidationSnapshot::default()
            }
            .mark_exchange_ready("gateio", "AURORAUSDT")
            .mark_exchange_ready("bitget", "AURORAUSDT"),
        )
        .await;
    let state = state.with_control_service(service);
    state
        .update_model(|model| {
            model.spot_symbol_rules = vec![
                test_symbol_rule("gateio", "AURORA", "USDT"),
                test_symbol_rule("bitget", "AURORA", "USDT"),
            ];
            model.books.push(BookView {
                exchange: "gateio".to_string(),
                market_type: MarketType::Spot,
                symbol: "AURORAUSDT".to_string(),
                exchange_symbol: "AURORA_USDT".to_string(),
                best_bid: Some(1.0),
                best_ask: Some(1.01),
                spread: Some(0.01),
                book_age_ms: 10,
                latency_ms: Some(1),
                is_stale: false,
                stale_reason: None,
                source: "websocket".to_string(),
                sequence: Some(10),
            });
            model.books.push(BookView {
                exchange: "bitget".to_string(),
                market_type: MarketType::Spot,
                symbol: "AURORAUSDT".to_string(),
                exchange_symbol: "AURORAUSDT".to_string(),
                best_bid: Some(1.02),
                best_ask: Some(1.03),
                spread: Some(0.01),
                book_age_ms: 10,
                latency_ms: Some(1),
                is_stale: false,
                stale_reason: None,
                source: "websocket".to_string(),
                sequence: Some(11),
            });
            for exchange in ["gateio", "bitget"] {
                model.inventory.push(InventoryView {
                    exchange: exchange.to_string(),
                    market_type: MarketType::Spot,
                    asset: "USDT".to_string(),
                    total: 100.0,
                    available: 100.0,
                    locked_by_exchange: 0.0,
                    locally_reserved: 0.0,
                    effective_available: 100.0,
                    unmanaged_quantity: 0.0,
                    valuation_usdt: Some(100.0),
                });
                model.inventory.push(InventoryView {
                    exchange: exchange.to_string(),
                    market_type: MarketType::Spot,
                    asset: "AURORA".to_string(),
                    total: 20.0,
                    available: 20.0,
                    locked_by_exchange: 0.0,
                    locally_reserved: 0.0,
                    effective_available: 20.0,
                    unmanaged_quantity: 0.0,
                    valuation_usdt: Some(20.0),
                });
                model.fees.push(FeeView {
                    exchange: exchange.to_string(),
                    market_type: MarketType::Spot,
                    symbol: Some("AURORAUSDT".to_string()),
                    maker_fee_bps: 10.0,
                    taker_fee_bps: 10.0,
                    source: FeeSource::SymbolOverride,
                    platform_discount_enabled: false,
                    platform_token: None,
                    updated_at: Utc::now(),
                });
                model.exchanges.push(ExchangeHealthView {
                    exchange: exchange.to_string(),
                    market_type: Some(MarketType::Spot),
                    connected: true,
                    public_ws_connected: true,
                    private_ws_connected: Some(true),
                    last_message_at: Some(Utc::now()),
                    last_book_update_at: Some(Utc::now()),
                    stale_symbol_count: 0,
                    fresh_symbol_count: 1,
                    reconnect_count: 0,
                    parse_error_count: 0,
                    sequence_gap_count: 0,
                    heartbeat_timeout_count: 0,
                    avg_latency_ms: Some(1.0),
                    max_latency_ms: Some(2),
                });
            }
            model.balance_reconciliation = Some(crate::execution::reconcile_dashboard_inventory(
                &model.inventory,
            ));
        })
        .await;
    state
}

fn test_symbol_rule(exchange: &str, base: &str, quote: &str) -> SymbolRule {
    SymbolRule {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        internal_symbol: format!("{base}{quote}"),
        exchange_symbol: format!("{base}{quote}"),
        base_asset: base.to_string(),
        quote_asset: quote.to_string(),
        price_precision: 4,
        quantity_precision: 4,
        tick_size: 0.0001,
        step_size: 0.0001,
        min_quantity: 0.0001,
        min_notional: 1.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Limit, OrderType::IOC, OrderType::PostOnly],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::GTX],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

fn arbitrage_analysis() -> ArbitrageOpportunityAnalysis {
    ArbitrageOpportunityAnalysis {
        opportunity_id: "arb-1".to_string(),
        timestamp: Utc::now(),
        relationship_type: ArbitrageRelationshipType::SpotSpot,
        symbol: "BTCUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        buy_market_type: MarketType::Spot,
        sell_exchange: "coinex".to_string(),
        sell_market_type: MarketType::Spot,
        target_quantity: 0.1,
        target_notional: 10.0,
        buy_best_price: Some(100.0),
        sell_best_price: Some(101.0),
        buy_vwap: Some(100.1),
        sell_vwap: Some(100.9),
        buy_slippage_bps: 10.0,
        sell_slippage_bps: 10.0,
        raw_executable_spread_bps: 80.0,
        tt_immediate_net_bps: 50.0,
        tt_immediate_net_pnl: 0.05,
        tt_lifecycle_expected_net_bps: 35.0,
        tt_lifecycle_expected_net_pnl: 0.035,
        maker_buy_taker_sell_theoretical_net_bps: Some(70.0),
        maker_buy_taker_sell_expected_net_bps: Some(5.0),
        taker_buy_maker_sell_theoretical_net_bps: Some(65.0),
        taker_buy_maker_sell_expected_net_bps: Some(4.0),
        expected_funding_net_bps: 0.0,
        expected_rebalance_cost_bps: 5.0,
        expected_exit_fee_bps: 20.0,
        expected_exit_slippage_bps: 5.0,
        expected_residual_loss_bps: 5.0,
        safety_buffer_bps: 5.0,
        expected_holding_seconds: 3600,
        required_capital_usdt: 20.0,
        expected_return_on_capital: 0.00175,
        expected_return_on_capital_per_hour: 0.00175,
        risk_adjusted_score: 42.0,
        score_components: RiskScoreComponents {
            expected_return_on_capital_per_hour_score: 17.5,
            executable_depth_score: 15.0,
            book_freshness_score: 15.0,
            fee_confidence_score: 10.0,
            convergence_confidence_score: 5.0,
            funding_confidence_score: 10.0,
            residual_risk_penalty: 5.0,
            adverse_selection_penalty: 10.0,
            capital_fragmentation_penalty: 0.0,
            stale_book_penalty: 0.0,
            fee_fallback_penalty: 0.0,
            total_score: 42.0,
        },
        fee_sources: vec![FeeSource::ConfigDefault],
        book_ages: vec![10, 11],
        book_latencies: vec![Some(1), Some(2)],
        account_structure: AccountStructure::SeparateAccounts,
        confidence: ConfidenceLevel::Medium,
        warnings: vec!["maker/taker theoretical is non-executable".to_string()],
        rejection_reasons: Vec::new(),
        accepted: true,
    }
}

async fn get_json(path: &str, state: MonitoringState) -> (StatusCode, Value) {
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    };
    (status, value)
}

async fn get_json_with_token(
    path: &str,
    state: MonitoringState,
    token: &str,
) -> (StatusCode, Value) {
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    };
    (status, value)
}

async fn post_json(
    path: &str,
    state: MonitoringState,
    token: Option<&str>,
    body: &str,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = router(state)
        .oneshot(request.body(Body::from(body.to_string())).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    };
    (status, value)
}

async fn post_json_with_idempotency(
    path: &str,
    state: MonitoringState,
    token: Option<&str>,
    idempotency_key: Option<&str>,
    body: &str,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(idempotency_key) = idempotency_key {
        request = request.header("Idempotency-Key", idempotency_key);
    }
    let response = router(state)
        .oneshot(request.body(Body::from(body.to_string())).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    };
    (status, value)
}

#[tokio::test]
async fn status_endpoint_should_return_json() {
    let (status, value) = get_json("/api/status", test_state(false)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["service_name"], "RustCTA");
}

#[tokio::test]
async fn exchanges_endpoint_should_return_health() {
    let (_, value) = get_json("/api/exchanges", test_state(false)).await;
    assert_eq!(value.as_array().unwrap()[0]["exchange"], "mexc");
}

#[tokio::test]
async fn books_endpoint_should_return_snapshots_and_filter_stale() {
    let (_, all) = get_json("/api/books", test_state(false)).await;
    assert_eq!(all.as_array().unwrap().len(), 2);
    let (_, stale) = get_json("/api/books?stale_only=true", test_state(false)).await;
    assert_eq!(stale.as_array().unwrap().len(), 1);
    assert_eq!(stale.as_array().unwrap()[0]["exchange"], "coinex");
}

#[tokio::test]
async fn recent_opportunities_endpoint_should_return_records() {
    let (_, value) = get_json("/api/opportunities/recent", test_state(false)).await;
    assert_eq!(value.as_array().unwrap()[0]["opportunity_id"], "opp-1");
}

#[tokio::test]
async fn recent_trades_endpoint_should_return_paper_trades() {
    let (_, value) = get_json("/api/trades/recent", test_state(false)).await;
    assert_eq!(value.as_array().unwrap()[0]["paper_or_live"], "paper");
}

#[tokio::test]
async fn disabled_and_unmanaged_endpoints_should_return_entries() {
    let (_, disabled) = get_json("/api/disabled", test_state(false)).await;
    assert_eq!(disabled["symbols"][0]["symbol"], "TURBOSUSDT");
    let (_, unmanaged) = get_json("/api/unmanaged_positions", test_state(false)).await;
    assert_eq!(unmanaged.as_array().unwrap()[0]["asset"], "POND");
}

#[tokio::test]
async fn fees_endpoint_should_return_fee_summary() {
    let (_, value) = get_json("/api/fees", test_state(false)).await;
    assert_eq!(value.as_array().unwrap()[0]["taker_fee_bps"], 5.0);
}

#[tokio::test]
async fn config_summary_should_redact_secrets() {
    let (_, value) = get_json("/api/config/summary", test_state(false)).await;
    assert_eq!(value["secrets_redacted"], true);
    assert!(value.get("api_secret").is_none());
}

#[tokio::test]
async fn live_preflight_endpoint_should_return_json() {
    let (_, value) = get_json("/api/live_preflight", test_state(false)).await;
    assert_eq!(value["pass_count"], 1);
    let (_, checks) = get_json("/api/live_preflight/checks", test_state(false)).await;
    assert_eq!(checks.as_array().unwrap()[0]["name"], "test_check");
    let (_, summary) = get_json("/api/live_preflight/summary", test_state(false)).await;
    assert_eq!(summary["pass_count"], 1);
}

#[tokio::test]
async fn token_auth_should_reject_missing_token() {
    std::env::set_var("RUSTCTA_MONITOR_TEST_TOKEN", "secret");
    let response = router(test_state(true))
        .oneshot(
            Request::builder()
                .uri("/api/status")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn kill_switch_post_should_require_token_auth() {
    std::env::set_var("RUSTCTA_MONITOR_TEST_TOKEN", "secret");
    let (status, _) = post_json(
        "/api/kill_switch/trigger",
        test_state(true),
        None,
        r#"{"reason":"test"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, value) = post_json(
        "/api/kill_switch/trigger",
        test_state(true),
        Some("secret"),
        r#"{"reason":"test","triggered_by":"unit"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["active"], true);
}

#[tokio::test]
async fn kill_switch_post_should_be_unavailable_without_token_auth() {
    let (status, _) = post_json(
        "/api/kill_switch/trigger",
        test_state(false),
        None,
        r#"{"reason":"test"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn control_write_endpoints_should_require_token() {
    std::env::set_var("RUSTCTA_MONITOR_TEST_TOKEN", "secret");
    let (status, _) = post_json_with_idempotency(
        "/api/control/symbols/enable",
        control_state(true).await,
        None,
        Some("idem-control-auth"),
        r#"{
            "symbol":"AURORAUSDT",
            "mode":"LiveDryRun",
            "selected_exchanges":["gateio","bitget"],
            "allowed_directions":[{"buy_exchange":"gateio","sell_exchange":"bitget"}],
            "expected_version":0
        }"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn control_write_endpoints_should_be_unavailable_when_auth_disabled() {
    let (status, _) = post_json_with_idempotency(
        "/api/control/symbols/enable",
        control_state(false).await,
        None,
        Some("idem-auth-disabled"),
        r#"{
            "symbol":"AURORAUSDT",
            "mode":"LiveDryRun",
            "selected_exchanges":["gateio","bitget"],
            "allowed_directions":[{"buy_exchange":"gateio","sell_exchange":"bitget"}],
            "expected_version":0
        }"#,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn control_write_endpoints_should_require_idempotency_key() {
    std::env::set_var("RUSTCTA_MONITOR_TEST_TOKEN", "secret");
    let (status, _) = post_json_with_idempotency(
        "/api/control/symbols/enable",
        control_state(true).await,
        Some("secret"),
        None,
        r#"{
            "symbol":"AURORAUSDT",
            "mode":"LiveDryRun",
            "selected_exchanges":["gateio","bitget"],
            "allowed_directions":[{"buy_exchange":"gateio","sell_exchange":"bitget"}],
            "expected_version":0
        }"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn control_enable_should_be_idempotent_and_audited() {
    std::env::set_var("RUSTCTA_MONITOR_TEST_TOKEN", "secret");
    let state = control_state(true).await;
    let body = r#"{
        "symbol":"AURORAUSDT",
        "mode":"LiveDryRun",
        "selected_exchanges":["gateio","bitget"],
        "allowed_directions":[{"buy_exchange":"gateio","sell_exchange":"bitget"}],
        "expected_version":0
    }"#;
    let (first_status, first) = post_json_with_idempotency(
        "/api/control/symbols/enable",
        state.clone(),
        Some("secret"),
        Some("idem-enable-api"),
        body,
    )
    .await;
    let (second_status, second) = post_json_with_idempotency(
        "/api/control/symbols/enable",
        state.clone(),
        Some("secret"),
        Some("idem-enable-api"),
        body,
    )
    .await;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(first["command_id"], second["command_id"]);
    assert_eq!(first["lifecycle_state"], "Active");

    let (_, audit) = get_json_with_token("/api/control/audit", state, "secret").await;
    assert!(audit
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["action"] == "Enable" && event["result"] == "Completed"));
}

#[tokio::test]
async fn control_expected_version_should_prevent_stale_update() {
    std::env::set_var("RUSTCTA_MONITOR_TEST_TOKEN", "secret");
    let state = control_state(true).await;
    let enable_body = r#"{
        "symbol":"AURORAUSDT",
        "mode":"LiveDryRun",
        "selected_exchanges":["gateio","bitget"],
        "allowed_directions":[{"buy_exchange":"gateio","sell_exchange":"bitget"}],
        "expected_version":0
    }"#;
    let _ = post_json_with_idempotency(
        "/api/control/symbols/enable",
        state.clone(),
        Some("secret"),
        Some("idem-enable-stale-api"),
        enable_body,
    )
    .await;
    let (status, value) = post_json_with_idempotency(
        "/api/control/symbols/AURORAUSDT/pause",
        state,
        Some("secret"),
        Some("idem-pause-stale-api"),
        r#"{"expected_version":0}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["status"], "Rejected");
    assert_eq!(
        value["validation_errors"][0]["code"],
        "stale_lifecycle_version"
    );
}

#[tokio::test]
async fn token_auth_should_accept_valid_token() {
    std::env::set_var("RUSTCTA_MONITOR_TEST_TOKEN", "secret");
    let response = router(test_state(true))
        .oneshot(
            Request::builder()
                .uri("/api/status")
                .header("Authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn disabled_dashboard_should_not_start_server() {
    let config = MonitoringConfig::default();
    let state = MonitoringState::new(config.clone());
    let handle = spawn_monitoring_server(config, state).await.unwrap();
    assert!(handle.is_none());
}

#[tokio::test]
async fn dashboard_updates_should_not_block_strategy_loop_when_locked() {
    let state = test_state(false);
    let _guard = state.snapshot().await;
    state.record_risk_event(RiskEventView {
        timestamp: Utc::now(),
        event_type: "rejection".to_string(),
        symbol: Some("BTCUSDT".to_string()),
        exchange: Some("mexc".to_string()),
        severity: "warning".to_string(),
        reason: "stale_book".to_string(),
        details: None,
    });
}

#[tokio::test]
async fn arbitrage_opportunities_endpoint_should_return_key_fields() {
    let (status, value) = get_json("/api/arbitrage/opportunities", test_state(false)).await;
    assert_eq!(status, StatusCode::OK);
    let item = &value.as_array().unwrap()[0];
    assert_eq!(item["relationship_type"], "spot_spot");
    assert_eq!(item["buy_vwap"], 100.1);
    assert_ne!(
        item["raw_executable_spread_bps"],
        item["tt_immediate_net_bps"]
    );
    assert_eq!(item["confidence"], "medium");
    assert!(item["score_components"]["total_score"].as_f64().unwrap() > 0.0);
}

#[tokio::test]
async fn arbitrage_opportunity_filters_should_work() {
    let (_, value) = get_json(
        "/api/arbitrage/opportunities?relationship_type=spot_spot&symbol=BTC_USDT&exchange=mexc&min_tt_net_bps=10&confidence=low&accepted=true&fresh_only=true",
        test_state(false),
    )
    .await;
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (_, value) = get_json(
        "/api/arbitrage/opportunities?min_lifecycle_expected_net_bps=1000",
        test_state(false),
    )
    .await;
    assert_eq!(value.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn arbitrage_detail_rankings_fees_and_capital_should_return_json() {
    let (_, detail) = get_json("/api/arbitrage/opportunities/arb-1", test_state(false)).await;
    assert_eq!(detail["opportunity_id"], "arb-1");
    let (_, rankings) = get_json("/api/arbitrage/rankings", test_state(false)).await;
    assert_eq!(rankings.as_array().unwrap()[0]["risk_adjusted_score"], 42.0);
    let (_, fees) = get_json("/api/arbitrage/fees", test_state(false)).await;
    assert_eq!(
        fees["opportunity_fee_sources"].as_array().unwrap()[0]["fee_sources"]
            .as_array()
            .unwrap()[0],
        "config_default"
    );
    let (_, capital) = get_json("/api/arbitrage/capital-efficiency", test_state(false)).await;
    assert_eq!(
        capital.as_array().unwrap()[0]["required_capital_usdt"],
        20.0
    );
}

#[tokio::test]
async fn arbitrage_statistics_endpoint_should_return_confidence() {
    let (_, value) = get_json("/api/arbitrage/statistics", test_state(false)).await;
    assert_eq!(value.as_array().unwrap()[0]["confidence"], "medium");
}
