use chrono::Utc;

use crate::exchanges::unified::{MarketType, OrderType, SymbolRule, SymbolStatus, TimeInForce};
use crate::execution::FeeSource;
use crate::web::{
    BookView, ExchangeHealthView, FeeView, InventoryView, MonitoringConfig, RecorderHealthView,
};

use super::*;

fn config() -> LivePreflightConfig {
    LivePreflightConfig {
        enabled: true,
        exchanges: vec!["mexc".to_string()],
        symbols: vec!["BTCUSDT".to_string()],
        max_live_notional_per_trade: Some(5.0),
        max_total_live_notional: Some(20.0),
        require_monitoring_enabled: true,
        require_recorder_enabled: true,
        require_api_key_read_permission: true,
        require_withdraw_permission_absent: true,
        ..LivePreflightConfig::default()
    }
}

fn rule() -> SymbolRule {
    SymbolRule {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: "BTCUSDT".to_string(),
        exchange_symbol: "BTCUSDT".to_string(),
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        price_precision: 2,
        quantity_precision: 6,
        tick_size: 0.01,
        step_size: 0.000001,
        min_quantity: 0.000001,
        min_notional: 1.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Market, OrderType::Limit],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

fn state() -> LiveReadinessState {
    let mut state = LiveReadinessState {
        trading_mode: "paper".to_string(),
        live_trading_enabled: false,
        dry_run: true,
        monitoring_enabled: true,
        recorder_enabled: true,
        kill_switch_available: true,
        kill_switch_active: false,
        emergency_stop_configured: true,
        max_daily_loss: Some(100.0),
        max_order_latency_ms: Some(1_000),
        symbol_rules: vec![rule()],
        inventory: vec![InventoryView {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            asset: "USDT".to_string(),
            total: 20.0,
            available: 20.0,
            locked_by_exchange: 0.0,
            locally_reserved: 0.0,
            effective_available: 20.0,
            unmanaged_quantity: 0.0,
            valuation_usdt: Some(20.0),
        }],
        fees: vec![FeeView {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: None,
            maker_fee_bps: 0.0,
            taker_fee_bps: 5.0,
            source: FeeSource::ConfigDefault,
            platform_discount_enabled: false,
            platform_token: None,
            updated_at: Utc::now(),
        }],
        books: vec![BookView {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            exchange_symbol: "BTCUSDT".to_string(),
            best_bid: Some(99.0),
            best_ask: Some(100.0),
            spread: Some(1.0),
            book_age_ms: 100,
            latency_ms: Some(2),
            is_stale: false,
            stale_reason: None,
            source: "websocket".to_string(),
            sequence: Some(1),
        }],
        exchanges: vec![ExchangeHealthView {
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
        }],
        recorder: RecorderHealthView {
            book_recording_enabled: true,
            opportunity_recording_enabled: true,
            trade_recording_enabled: true,
            ..RecorderHealthView::default()
        },
        ..LiveReadinessState::default()
    };
    state.api_permissions.insert(
        "mexc".to_string(),
        ApiPermissionState {
            key_present: true,
            secret_present: true,
            read_permission: Some(true),
            trade_permission: None,
            withdraw_permission: Some(false),
            account_read_probe_ok: Some(true),
        },
    );
    state
}

fn statuses(report: &LivePreflightReport, name: &str) -> Vec<LivePreflightCheckStatus> {
    report
        .checks
        .iter()
        .filter(|check| check.name == name)
        .map(|check| check.status)
        .collect()
}

#[test]
fn config_safety_should_pass_in_paper_mode() {
    let report = run_live_preflight(config(), &state());
    assert!(statuses(&report, "trading_mode_safe").contains(&LivePreflightCheckStatus::Pass));
}

#[test]
fn config_safety_should_fail_if_live_trading_enabled_outside_live_mode() {
    let mut state = state();
    state.live_trading_enabled = true;
    let report = run_live_preflight(config(), &state);
    assert!(statuses(&report, "live_trading_flag").contains(&LivePreflightCheckStatus::Fail));
}

#[test]
fn config_safety_should_pass_in_live_mode_with_real_order_flags() {
    let mut state = state();
    state.trading_mode = "live".to_string();
    state.live_trading_enabled = true;
    state.dry_run = false;
    let mut config = config();
    config.require_api_key_trade_permission = true;
    let report = run_live_preflight(config, &state);
    assert!(statuses(&report, "trading_mode_safe").contains(&LivePreflightCheckStatus::Pass));
    assert!(statuses(&report, "live_trading_flag").contains(&LivePreflightCheckStatus::Pass));
    assert!(statuses(&report, "dry_run_mode").contains(&LivePreflightCheckStatus::Pass));
}

#[test]
fn config_safety_should_fail_if_max_trade_notional_missing() {
    let mut config = config();
    config.max_live_notional_per_trade = None;
    let report = run_live_preflight(config, &state());
    assert!(statuses(&report, "small_trade_notional").contains(&LivePreflightCheckStatus::Fail));
}

#[test]
fn api_permission_unknown_should_warn_not_pass() {
    let mut state = state();
    state
        .api_permissions
        .get_mut("mexc")
        .unwrap()
        .read_permission = None;
    let report = run_live_preflight(config(), &state);
    assert!(statuses(&report, "api_read_permission").contains(&LivePreflightCheckStatus::Unknown));
}

#[test]
fn missing_symbol_rule_should_fail() {
    let mut state = state();
    state.symbol_rules.clear();
    let report = run_live_preflight(config(), &state);
    assert!(statuses(&report, "symbol_rule_available").contains(&LivePreflightCheckStatus::Fail));
}

#[test]
fn stale_book_should_fail() {
    let mut state = state();
    state.books[0].is_stale = true;
    state.books[0].book_age_ms = 4_300;
    let report = run_live_preflight(config(), &state);
    assert!(statuses(&report, "book_fresh").contains(&LivePreflightCheckStatus::Fail));
}

#[test]
fn missing_fee_model_should_fail() {
    let mut state = state();
    state.fees.clear();
    let report = run_live_preflight(config(), &state);
    assert!(statuses(&report, "fee_model_available").contains(&LivePreflightCheckStatus::Fail));
}

#[test]
fn disabled_symbol_should_fail() {
    let mut state = state();
    state.disabled_symbols.push("BTCUSDT".to_string());
    let report = run_live_preflight(config(), &state);
    assert!(statuses(&report, "symbol_not_disabled").contains(&LivePreflightCheckStatus::Fail));
}

#[test]
fn unmanaged_position_overlap_should_fail() {
    let mut state = state();
    state.unmanaged_positions.push((
        "mexc".to_string(),
        MarketType::Spot,
        "BTCUSDT".to_string(),
        "BTC".to_string(),
        0.1,
    ));
    let report = run_live_preflight(config(), &state);
    assert!(statuses(&report, "unmanaged_overlap").contains(&LivePreflightCheckStatus::Fail));
}

#[test]
fn fresh_book_valid_symbol_and_fee_should_pass() {
    let report = run_live_preflight(config(), &state());
    assert!(!report.has_failures());
    assert!(statuses(&report, "book_fresh").contains(&LivePreflightCheckStatus::Pass));
    assert!(statuses(&report, "fee_model_available").contains(&LivePreflightCheckStatus::Pass));
    assert!(
        statuses(&report, "order_validation_readiness").contains(&LivePreflightCheckStatus::Pass)
    );
}

#[tokio::test]
async fn dashboard_live_preflight_endpoint_should_return_json() {
    let report = run_live_preflight(config(), &state());
    let state = crate::web::MonitoringState::new(MonitoringConfig {
        require_token: false,
        ..MonitoringConfig::default()
    });
    state.publish_live_preflight(report);
    let value = state.snapshot().await.live_preflight.unwrap();
    assert_eq!(value.decision, LiveReadinessDecision::ReadyForLiveDryRun);
}
