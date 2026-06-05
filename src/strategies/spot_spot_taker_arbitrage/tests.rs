use std::collections::HashMap;

use chrono::{Duration, Utc};
use tempfile::tempdir;

use super::*;
use crate::exchanges::unified::{
    MarketType, OrderBookLevel, OrderBookSnapshot, OrderType, SymbolRule, SymbolStatus, TimeInForce,
};
use crate::exchanges::{coinex, mexc};
use crate::execution::{FeeConfig, FeeModel, FeePairConfig, FeeSource, PlatformTokenDiscount};
use crate::risk::{
    DisabledConfig, DisabledExchangeSymbol, DisabledRegistry, DisabledRegistryConfig,
    DisabledSymbol, UnmanagedPosition,
};

fn config() -> SpotSpotTakerArbitrageConfig {
    let mut initial_balances = HashMap::new();
    initial_balances.insert(
        "mexc".to_string(),
        HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
    );
    initial_balances.insert(
        "coinex".to_string(),
        HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
    );
    SpotSpotTakerArbitrageConfig {
        enabled: true,
        trading_mode: "paper".to_string(),
        exchanges: vec!["mexc".to_string(), "coinex".to_string()],
        symbols: vec!["CUDISUSDT".to_string()],
        quote_asset: "USDT".to_string(),
        max_notional_per_trade: 100.0,
        min_notional_per_trade: 5.0,
        max_notional_per_symbol: 500.0,
        max_total_notional: 1_000.0,
        min_net_spread_bps: 10.0,
        taker_fee_bps_override: Some(10.0),
        fee_config_path: "config/fees.yml".to_string(),
        disabled_registry_path: "config/disabled_symbols.yml".to_string(),
        slippage_bps: 2.0,
        safety_buffer_bps: 3.0,
        stale_book_ms: 1_000,
        max_book_latency_ms: 1_000,
        min_depth_notional: 5.0,
        max_active_opportunities_per_symbol: 1,
        cooldown_ms_after_trade: 5_000,
        enable_database_recording: false,
        enable_csv_recording: false,
        report_interval_seconds: 30,
        dry_run: true,
        live_trading_enabled: false,
        scan_interval_ms: 1_000,
        orderbook_depth: 5,
        request_timeout_ms: 10_000,
        max_daily_loss: 100.0,
        max_trade_loss: 10.0,
        max_consecutive_rejections: 20,
        jsonl_path: "logs/test_spot_spot_taker_arbitrage.jsonl".to_string(),
        csv_path: "logs/test_spot_spot_taker_arbitrage.csv".to_string(),
        market_data_mode: MarketDataMode::RestPolling,
        websocket: WebsocketMarketDataConfig::default(),
        rest_polling: RestPollingMarketDataConfig::default(),
        replay: ReplayConfig::default(),
        monitoring: crate::web::MonitoringConfig::default(),
        spot_symbol_control: crate::control::spot_control::SpotSymbolControlConfig::default(),
        live_preflight: crate::live_preflight::LivePreflightConfig::default(),
        live_dry_run: crate::execution::LiveDryRunConfig::default(),
        order_reconciliation: crate::execution::OrderReconciliationConfig::default(),
        kill_switch: crate::risk::KillSwitchConfig::default(),
        small_live_gate: crate::live_preflight::SmallLiveGateConfig::default(),
        arbitrage_scanner: crate::strategies::arbitrage_core::ArbitrageScannerConfig::default(),
        initial_balances,
        mexc: VenueRuntimeConfig::default(),
        coinex: VenueRuntimeConfig::default(),
        gateio: VenueRuntimeConfig::default(),
        bitget: VenueRuntimeConfig::default(),
    }
}

fn build_with_models(
    cfg: &SpotSpotTakerArbitrageConfig,
    fee_model: &FeeModel,
    disabled_registry: &DisabledRegistry,
    buy_exchange: SpotVenue,
    sell_exchange: SpotVenue,
    buy_book: &OrderBookSnapshot,
    sell_book: &OrderBookSnapshot,
) -> OpportunityRecord {
    build_opportunity_with_source(
        cfg,
        &rules(),
        &PaperInventory::from_config(cfg).unwrap(),
        &RiskState::new(cfg),
        buy_exchange,
        sell_exchange,
        buy_book,
        sell_book,
        fee_model,
        disabled_registry,
        BookSource::Rest,
        BookSource::Rest,
    )
}

fn rule(exchange: &str) -> SymbolRule {
    SymbolRule {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        internal_symbol: "CUDISUSDT".to_string(),
        exchange_symbol: "CUDISUSDT".to_string(),
        base_asset: "CUDIS".to_string(),
        quote_asset: "USDT".to_string(),
        price_precision: 4,
        quantity_precision: 0,
        tick_size: 0.0001,
        step_size: 1.0,
        min_quantity: 1.0,
        min_notional: 5.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Market, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::IOC],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

fn rules() -> CommonSymbolRules {
    CommonSymbolRules {
        mexc: rule("mexc"),
        coinex: rule("coinex"),
        gateio: None,
        bitget: None,
    }
}

fn book(exchange: &str, bid: f64, ask: f64, qty: f64) -> OrderBookSnapshot {
    OrderBookSnapshot {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        symbol: "CUDISUSDT".to_string(),
        bids: vec![OrderBookLevel {
            price: bid,
            quantity: qty,
        }],
        asks: vec![OrderBookLevel {
            price: ask,
            quantity: qty,
        }],
        best_bid: Some(bid),
        best_ask: Some(ask),
        exchange_timestamp: Some(Utc::now()),
        received_at: Utc::now(),
        latency_ms: Some(10),
        sequence: Some(1),
        is_stale: false,
    }
}

#[test]
fn raw_spread_calculation_should_use_buy_price_denominator() {
    let spread = calculate_spread(1.0, 1.02, 0.0, 0.0, 0.0, 0.0);
    assert!((spread.raw_spread_bps - 200.0).abs() < 1e-9);
}

#[test]
fn net_spread_calculation_should_deduct_fees_slippage_and_buffer() {
    let spread = calculate_spread(1.0, 1.02, 10.0, 10.0, 2.0, 3.0);
    assert!((spread.net_spread_bps - 175.0).abs() < 1e-9);
}

#[test]
fn stale_order_book_should_reject_opportunity() {
    let cfg = config();
    let mut stale = book("mexc", 0.99, 1.0, 100.0);
    stale.received_at = Utc::now() - Duration::milliseconds(2_000);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &stale,
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::StaleBook));
}

#[test]
fn insufficient_depth_should_reject_opportunity() {
    let cfg = config();
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 2.0),
        &book("coinex", 1.03, 1.04, 2.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::InsufficientDepth)
    );
}

#[test]
fn min_notional_should_reject_opportunity() {
    let mut cfg = config();
    cfg.min_depth_notional = 1.0;
    cfg.min_notional_per_trade = 50.0;
    cfg.max_notional_per_trade = 20.0;
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 20.0),
        &book("coinex", 1.03, 1.04, 20.0),
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::MinNotional));
}

#[test]
fn symbol_rule_validation_should_reject_bad_step() {
    let mut cfg = config();
    cfg.max_notional_per_trade = 10.5;
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::SymbolRule));
}

#[test]
fn taker_buy_simulation_should_consume_asks() {
    let leg = simulate_taker_buy(
        &[
            OrderBookLevel {
                price: 1.0,
                quantity: 5.0,
            },
            OrderBookLevel {
                price: 1.1,
                quantity: 5.0,
            },
        ],
        10.0,
        10.0,
    )
    .unwrap();
    assert!((leg.average_price - 1.05).abs() < 1e-9);
    assert!((leg.fee - 0.0105).abs() < 1e-9);
}

#[test]
fn taker_sell_simulation_should_consume_bids() {
    let leg = simulate_taker_sell(
        &[
            OrderBookLevel {
                price: 1.1,
                quantity: 5.0,
            },
            OrderBookLevel {
                price: 1.0,
                quantity: 5.0,
            },
        ],
        10.0,
        10.0,
    )
    .unwrap();
    assert!((leg.average_price - 1.05).abs() < 1e-9);
}

#[test]
fn full_taker_taker_paper_trade_should_settle_inventory() {
    let cfg = config();
    let mut inventory = PaperInventory::from_config(&cfg).unwrap();
    let mexc = book("mexc", 0.99, 1.0, 100.0);
    let coinex = book("coinex", 1.03, 1.04, 100.0);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &inventory,
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &mexc,
        &coinex,
    );
    assert!(opp.accepted);
    let trade =
        execute_paper_taker_taker(&cfg, &mut inventory, &opp, &rules(), &mexc, &coinex).unwrap();
    assert!(trade.net_pnl > 0.0);
    assert!(inventory.realized_pnl > 0.0);
}

#[test]
fn spot_spot_taker_arbitrage_should_use_fee_model_for_net_spread() {
    let mut cfg = config();
    cfg.taker_fee_bps_override = None;
    let fee_model = FeeModel::default();
    let opp = build_with_models(
        &cfg,
        &fee_model,
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(opp.buy_fee_bps, 5.0);
    assert_eq!(opp.sell_fee_bps, 20.0);
    assert_eq!(opp.fee_source_buy, FeeSource::ConfigDefault);
    assert_eq!(opp.fee_source_sell, FeeSource::ConfigDefault);
}

#[test]
fn opportunity_should_be_accepted_when_fees_allow_it() {
    let cfg = config();
    let opp = build_with_models(
        &cfg,
        &fee_model_from_strategy_config(&cfg),
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert!(opp.accepted);
    assert!(opp.estimated_net_pnl > 0.0);
}

#[test]
fn opportunity_should_be_rejected_when_fee_model_makes_spread_negative() {
    let mut cfg = config();
    cfg.taker_fee_bps_override = None;
    cfg.min_net_spread_bps = 0.0;
    let fee_model = FeeModel::from_config(FeeConfig {
        fallback: HashMap::new(),
        defaults: HashMap::from([
            (
                "mexc".to_string(),
                HashMap::from([(
                    "spot".to_string(),
                    FeePairConfig {
                        maker_bps: 0.0,
                        taker_bps: 200.0,
                        fee_asset: Some("quote".to_string()),
                    },
                )]),
            ),
            (
                "coinex".to_string(),
                HashMap::from([(
                    "spot".to_string(),
                    FeePairConfig {
                        maker_bps: 0.0,
                        taker_bps: 200.0,
                        fee_asset: Some("quote".to_string()),
                    },
                )]),
            ),
        ]),
        symbol_overrides: Vec::new(),
        vip_overrides: Vec::new(),
        exchange_api: Vec::new(),
        platform_tokens: Vec::new(),
        prefer_exchange_api_fees: false,
    });
    let opp = build_with_models(
        &cfg,
        &fee_model,
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::NetSpreadBelowThreshold)
    );
}

#[test]
fn opportunity_should_be_rejected_by_disabled_symbol() {
    let cfg = config();
    let disabled = DisabledRegistry::from_config(DisabledRegistryConfig {
        disabled: DisabledConfig {
            symbols: vec![DisabledSymbol {
                symbol: "CUDISUSDT".to_string(),
                reason: "manual disabled".to_string(),
                expires_at: None,
            }],
            exchanges: Vec::new(),
            exchange_symbols: Vec::new(),
        },
        unmanaged_positions: Vec::new(),
    });
    let opp = build_with_models(
        &cfg,
        &fee_model_from_strategy_config(&cfg),
        &disabled,
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::DisabledSymbol));
    assert_eq!(opp.rejection_detail.as_deref(), Some("manual disabled"));
}

#[test]
fn opportunity_should_be_rejected_by_disabled_exchange_symbol() {
    let cfg = config();
    let disabled = DisabledRegistry::from_config(DisabledRegistryConfig {
        disabled: DisabledConfig {
            symbols: Vec::new(),
            exchanges: Vec::new(),
            exchange_symbols: vec![DisabledExchangeSymbol {
                exchange: "mexc".to_string(),
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                reason: "oversold errors".to_string(),
                expires_at: None,
            }],
        },
        unmanaged_positions: Vec::new(),
    });
    let opp = build_with_models(
        &cfg,
        &fee_model_from_strategy_config(&cfg),
        &disabled,
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::DisabledExchangeSymbol)
    );
    assert_eq!(opp.rejection_detail.as_deref(), Some("oversold errors"));
}

#[test]
fn unmanaged_position_should_be_excluded_from_inventory() {
    let cfg = config();
    let disabled = DisabledRegistry::from_config(DisabledRegistryConfig {
        disabled: DisabledConfig::default(),
        unmanaged_positions: vec![UnmanagedPosition {
            exchange: "coinex".to_string(),
            market_type: MarketType::Spot,
            symbol: "CUDISUSDT".to_string(),
            asset: "CUDIS".to_string(),
            quantity: 9_950.0,
            reason: "legacy residual".to_string(),
            created_at: Utc::now(),
        }],
    });
    let mut inventory = PaperInventory::from_config(&cfg).unwrap();
    inventory.exclude_unmanaged_positions(&disabled);
    assert_eq!(
        inventory
            .balance(SpotVenue::CoinEx, "CUDIS")
            .effective_available(),
        50.0
    );
}

#[test]
fn opportunity_record_should_include_platform_discount_fee_source() {
    let mut cfg = config();
    cfg.taker_fee_bps_override = None;
    let mut fee_config = FeeConfig::default();
    fee_config.platform_tokens.push(PlatformTokenDiscount {
        exchange: "mexc".to_string(),
        token: "MX".to_string(),
        enabled: true,
        discount_multiplier: Some(0.5),
    });
    let opp = build_with_models(
        &cfg,
        &FeeModel::from_config(fee_config),
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert!(opp.platform_discount_applied);
    assert_eq!(opp.fee_source_buy, FeeSource::PlatformTokenDiscount);
}

#[test]
fn insufficient_quote_balance_should_reject() {
    let mut cfg = config();
    cfg.initial_balances
        .get_mut("mexc")
        .unwrap()
        .insert("USDT".to_string(), 1.0);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::InsufficientQuoteBalance)
    );
}

#[test]
fn insufficient_base_balance_should_reject() {
    let mut cfg = config();
    cfg.initial_balances
        .get_mut("coinex")
        .unwrap()
        .insert("CUDIS".to_string(), 1.0);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::InsufficientBaseBalance)
    );
}

#[test]
fn fee_deduction_should_reduce_net_pnl() {
    let leg = simulate_taker_buy(
        &[OrderBookLevel {
            price: 1.0,
            quantity: 10.0,
        }],
        10.0,
        10.0,
    )
    .unwrap();
    assert_eq!(leg.notional, 10.0);
    assert_eq!(leg.fee, 0.01);
}

#[test]
fn inventory_settlement_should_update_balances() {
    let cfg = config();
    let mut inventory = PaperInventory::from_config(&cfg).unwrap();
    inventory.settle_buy(SpotVenue::Mexc, "CUDIS", "USDT", 10.0, 10.0, 0.01);
    assert!((inventory.balance(SpotVenue::Mexc, "USDT").available - 489.99).abs() < 1e-9);
    assert!((inventory.balance(SpotVenue::Mexc, "CUDIS").available - 10_010.0).abs() < 1e-9);
}

#[test]
fn opportunity_recording_should_write_jsonl() {
    let path = format!(
        "{}/spot_spot_taker_{}.jsonl",
        std::env::temp_dir().display(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let event = RecorderEvent::Opportunity(OpportunityRecord {
        timestamp: Utc::now(),
        symbol: "CUDISUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_price: 1.0,
        sell_price: 1.03,
        raw_spread_bps: 300.0,
        buy_fee_bps: 10.0,
        sell_fee_bps: 10.0,
        fee_source_buy: FeeSource::ConfigDefault,
        fee_source_sell: FeeSource::ConfigDefault,
        platform_discount_applied: false,
        estimated_fee_bps: 20.0,
        estimated_slippage_bps: 2.0,
        safety_buffer_bps: 3.0,
        estimated_net_spread_bps: 275.0,
        estimated_total_fee: 0.2,
        estimated_gross_pnl: 3.0,
        estimated_net_pnl: 2.8,
        executable_notional: 100.0,
        quantity: 100.0,
        accepted: true,
        rejection_reason: None,
        rejection_detail: None,
        buy_book_age_ms: 1,
        sell_book_age_ms: 1,
        buy_book_source: BookSource::Rest,
        sell_book_source: BookSource::Rest,
        buy_latency_ms: Some(1),
        sell_latency_ms: Some(1),
    });
    append_jsonl_for_test(&path, &event).unwrap();
    let raw = std::fs::read_to_string(path).unwrap();
    assert!(raw.contains("\"event_type\":\"opportunity\""));
}

#[test]
fn rejection_reason_recording_should_group_reasons() {
    let mut report = SummaryReport::default();
    report.record_rejection(RejectionReason::StaleBook);
    report.record_rejection(RejectionReason::StaleBook);
    assert_eq!(
        report
            .rejection_reasons
            .get(&RejectionReason::StaleBook)
            .copied(),
        Some(2)
    );
}

#[test]
fn live_trading_config_must_be_rejected() {
    let mut cfg = config();
    cfg.live_trading_enabled = true;
    assert!(cfg.validate_safe_mode().is_err());
    cfg.live_trading_enabled = false;
    cfg.trading_mode = "live".to_string();
    assert!(cfg.validate_safe_mode().is_err());
    cfg.trading_mode = "live_dry_run".to_string();
    assert!(cfg.validate_safe_mode().is_ok());
    cfg.live_dry_run.submit_orders = true;
    assert!(cfg.validate_safe_mode().is_err());
}

#[tokio::test]
async fn book_cache_insert_and_read_should_return_normalized_book() {
    let cache = BookCache::default();
    cache
        .update_book(book("MEXC", 0.99, 1.0, 100.0), BookSource::Websocket)
        .await;

    let cached = cache.get_book("mexc", "cudisusdt").await.unwrap();
    assert_eq!(cached.exchange, "mexc");
    assert_eq!(cached.symbol, "CUDISUSDT");
    assert_eq!(cached.source, BookSource::Websocket);
    assert_eq!(
        cache.get_best_bid_ask("MEXC", "CUDISUSDT").await,
        Some((0.99, 1.0))
    );
}

#[tokio::test]
async fn book_cache_stale_detection_should_use_age_and_flags() {
    let cache = BookCache::default();
    let mut snapshot = book("mexc", 0.99, 1.0, 100.0);
    snapshot.received_at = Utc::now() - Duration::milliseconds(2_000);
    cache.update_book(snapshot, BookSource::Websocket).await;

    assert!(!cache.is_fresh("mexc", "CUDISUSDT", 1_000).await);
    assert!(cache.get_book_age_ms("mexc", "CUDISUSDT").await.unwrap() >= 2_000);

    cache
        .update_book(book("mexc", 0.99, 1.0, 100.0), BookSource::Websocket)
        .await;
    assert!(cache.is_fresh("mexc", "CUDISUSDT", 1_000).await);
    cache.mark_stale("mexc", "CUDISUSDT").await;
    assert!(!cache.is_fresh("mexc", "CUDISUSDT", 1_000).await);
}

#[test]
fn mexc_websocket_update_normalization_should_parse_depth() {
    let ts = Utc::now().timestamp_millis();
    let message = serde_json::json!({
        "s": "CUDISUSDT",
        "d": {
            "bids": [["1.00", "10"]],
            "asks": [["1.01", "11"]],
            "E": ts,
            "u": 42
        }
    })
    .to_string();

    let snapshot = mexc::parse_ws_orderbook_message(&message, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.exchange, "mexc");
    assert_eq!(snapshot.symbol, "CUDISUSDT");
    assert_eq!(snapshot.best_bid, Some(1.0));
    assert_eq!(snapshot.best_ask, Some(1.01));
    assert_eq!(snapshot.sequence, Some(42));
}

#[test]
fn coinex_websocket_update_normalization_should_parse_depth() {
    let ts = Utc::now().timestamp_millis();
    let message = serde_json::json!({
        "params": {
            "market": "CUDISUSDT",
            "bids": [["1.00", "10"]],
            "asks": [["1.01", "11"]],
            "updated_at": ts,
            "last": 43
        }
    })
    .to_string();

    let snapshot = coinex::parse_ws_orderbook_message(&message, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.exchange, "coinex");
    assert_eq!(snapshot.symbol, "CUDISUSDT");
    assert_eq!(snapshot.best_bid, Some(1.0));
    assert_eq!(snapshot.best_ask, Some(1.01));
    assert_eq!(snapshot.sequence, Some(43));
}

#[test]
fn book_recorder_should_write_jsonl() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("books.jsonl");
    let cached = CachedBook {
        exchange: "mexc".to_string(),
        symbol: "CUDISUSDT".to_string(),
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 10.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.01,
            quantity: 10.0,
        }],
        best_bid: Some(1.0),
        best_ask: Some(1.01),
        exchange_timestamp: None,
        local_timestamp: Utc::now(),
        latency_ms: None,
        sequence: Some(1),
        source: BookSource::Websocket,
        is_stale: false,
    };
    append_book_record(
        path.to_str().unwrap(),
        &BookRecord::from_cached(&cached, true),
    )
    .unwrap();

    let raw = std::fs::read_to_string(path).unwrap();
    assert!(raw.contains("\"event_type\":\"top_of_book\""));
    assert!(raw.contains("\"source\":\"websocket\""));
}

#[test]
fn replay_should_load_jsonl_records() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("books.jsonl");
    let record = test_book_record("mexc", "CUDISUSDT", 1.0, 1.01, 10.0, 1);
    append_book_record(path.to_str().unwrap(), &record).unwrap();

    let records = load_book_records(path.to_str().unwrap()).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].timestamp_local(), record.timestamp_local());
}

#[tokio::test]
async fn replay_should_reconstruct_book_state() {
    let cache = BookCache::default();
    let record = test_book_record("coinex", "CUDISUSDT", 1.03, 1.04, 10.0, 2);
    let cached = record.into_cached();
    cache
        .update_book(cached.into_snapshot(), BookSource::Replay)
        .await;

    let replayed = cache.get_book("coinex", "CUDISUSDT").await.unwrap();
    assert_eq!(replayed.source, BookSource::Replay);
    assert_eq!(replayed.best_bid, Some(1.03));
    assert_eq!(replayed.sequence, Some(2));
}

#[tokio::test]
async fn opportunity_detection_from_replayed_books_should_accept_spread() {
    let cfg = replay_config_with_paths("unused", "unused");
    let cache = BookCache::default();
    cache
        .update_book(
            test_book_record("mexc", "CUDISUSDT", 0.99, 1.0, 200.0, 1)
                .into_cached()
                .into_snapshot(),
            BookSource::Replay,
        )
        .await;
    cache
        .update_book(
            test_book_record("coinex", "CUDISUSDT", 1.03, 1.04, 200.0, 1)
                .into_cached()
                .into_snapshot(),
            BookSource::Replay,
        )
        .await;

    let mexc_book = cache
        .get_book("mexc", "CUDISUSDT")
        .await
        .unwrap()
        .into_snapshot();
    let coinex_book = cache
        .get_book("coinex", "CUDISUSDT")
        .await
        .unwrap()
        .into_snapshot();
    let fee_model = fee_model_from_strategy_config(&cfg);
    let disabled_registry = DisabledRegistry::new();
    let opportunities = detect_opportunities_for_pair_with_source(
        &cfg,
        &rules(),
        &mexc_book,
        &coinex_book,
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        &fee_model,
        &disabled_registry,
        BookSource::Replay,
        BookSource::Replay,
    );
    assert!(opportunities.iter().any(|opportunity| opportunity.accepted));
    assert!(opportunities
        .iter()
        .all(|opportunity| opportunity.buy_book_source == BookSource::Replay));
}

#[test]
fn stale_books_should_be_rejected_in_websocket_cache_mode() {
    let mut cfg = config();
    cfg.market_data_mode = MarketDataMode::WebsocketCache;
    cfg.websocket.enabled = true;
    let mut stale = book("mexc", 0.99, 1.0, 100.0);
    stale.is_stale = true;
    let fee_model = fee_model_from_strategy_config(&cfg);
    let disabled_registry = DisabledRegistry::new();
    let opp = build_opportunity_with_source(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &stale,
        &book("coinex", 1.03, 1.04, 100.0),
        &fee_model,
        &disabled_registry,
        BookSource::Websocket,
        BookSource::Websocket,
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::StaleBook));
    assert_eq!(opp.buy_book_source, BookSource::Websocket);
}

#[tokio::test]
async fn replay_mode_should_run_without_network_clients() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("books.jsonl");
    let output = dir.path().join("report.jsonl");
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record("mexc", "CUDISUSDT", 0.99, 1.0, 200.0, 1),
    )
    .unwrap();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record("coinex", "CUDISUSDT", 1.03, 1.04, 200.0, 2),
    )
    .unwrap();
    let cfg = replay_config_with_paths(input.to_str().unwrap(), output.to_str().unwrap());

    ensure_replay_is_network_free(&cfg).unwrap();
    let report = run_replay_mode(cfg).await.unwrap();
    assert_eq!(report.total_book_events, 2);
    assert!(report.opportunities_detected > 0);
    assert!(output.exists());
}

#[tokio::test]
async fn replay_output_should_be_deterministic_for_same_input() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("books.jsonl");
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record("mexc", "CUDISUSDT", 0.99, 1.0, 200.0, 1),
    )
    .unwrap();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record("coinex", "CUDISUSDT", 1.03, 1.04, 200.0, 2),
    )
    .unwrap();

    let first_output = dir.path().join("report_1.jsonl");
    let second_output = dir.path().join("report_2.jsonl");
    let first = run_replay_mode(replay_config_with_paths(
        input.to_str().unwrap(),
        first_output.to_str().unwrap(),
    ))
    .await
    .unwrap();
    let second = run_replay_mode(replay_config_with_paths(
        input.to_str().unwrap(),
        second_output.to_str().unwrap(),
    ))
    .await
    .unwrap();

    assert_eq!(first.total_book_events, second.total_book_events);
    assert_eq!(first.opportunities_detected, second.opportunities_detected);
    assert_eq!(first.opportunities_accepted, second.opportunities_accepted);
    assert!((first.simulated_net_pnl - second.simulated_net_pnl).abs() < 1e-9);
}

#[test]
fn opportunity_duration_tracker_should_measure_continuity() {
    let mut first = test_opportunity_record(true);
    first.timestamp = Utc::now();
    let mut second = first.clone();
    second.timestamp = first.timestamp + Duration::milliseconds(125);
    second.estimated_net_spread_bps = 50.0;
    second.executable_notional = 75.0;
    let mut rejected = second.clone();
    rejected.timestamp = second.timestamp + Duration::milliseconds(1);
    rejected.accepted = false;
    rejected.rejection_reason = Some(RejectionReason::NetSpreadBelowThreshold);

    let mut tracker = OpportunityDurationTracker::default();
    tracker.observe(&first);
    tracker.observe(&second);
    tracker.observe(&rejected);
    let durations = tracker.finish();

    assert_eq!(durations.len(), 1);
    assert_eq!(durations[0].duration_ms, 125);
    assert_eq!(durations[0].max_net_spread_bps, 50.0);
    assert_eq!(durations[0].max_executable_notional, 75.0);
}

#[tokio::test]
async fn reconnect_should_mark_books_stale() {
    let cache = BookCache::default();
    cache
        .update_book(book("mexc", 0.99, 1.0, 100.0), BookSource::Websocket)
        .await;
    mark_reconnect_stale_for_test(&cache, SpotVenue::Mexc, &["CUDISUSDT".to_string()]).await;

    assert!(!cache.is_fresh("mexc", "CUDISUSDT", 1_000).await);
    assert!(cache.get_book("mexc", "CUDISUSDT").await.unwrap().is_stale);
}

#[test]
fn opportunity_record_should_include_book_age_ms() {
    let cfg = config();
    let mut mexc = book("mexc", 0.99, 1.0, 100.0);
    mexc.received_at = Utc::now() - Duration::milliseconds(25);
    let fee_model = fee_model_from_strategy_config(&cfg);
    let disabled_registry = DisabledRegistry::new();
    let opp = build_opportunity_with_source(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &mexc,
        &book("coinex", 1.03, 1.04, 100.0),
        &fee_model,
        &disabled_registry,
        BookSource::Websocket,
        BookSource::Websocket,
    );

    assert!(opp.buy_book_age_ms >= 25);
    assert!(opp.sell_book_age_ms >= 0);
}

#[test]
fn gateio_bitget_pair_should_build_live_dry_run_opportunities() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.exchanges = vec!["gateio".to_string(), "bitget".to_string()];
    cfg.websocket.exchanges = cfg.exchanges.clone();
    cfg.initial_balances = HashMap::from([
        (
            "gateio".to_string(),
            HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
        ),
        (
            "bitget".to_string(),
            HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
        ),
    ]);
    let gate_rule = rule("gateio");
    let bitget_rule = rule("bitget");
    let rules = CommonSymbolRules {
        mexc: gate_rule.clone(),
        coinex: bitget_rule.clone(),
        gateio: Some(gate_rule),
        bitget: Some(bitget_rule),
    };
    let gate_book = book("gateio", 0.9999, 1.0000, 1_000.0);
    let bitget_book = book("bitget", 1.0300, 1.0301, 1_000.0);

    let opportunities = detect_opportunities_for_pair_with_source(
        &cfg,
        &rules,
        &gate_book,
        &bitget_book,
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        &fee_model_from_strategy_config(&cfg),
        &DisabledRegistry::new(),
        BookSource::Websocket,
        BookSource::Websocket,
    );

    assert_eq!(opportunities[0].buy_exchange, "gateio");
    assert_eq!(opportunities[0].sell_exchange, "bitget");
    assert!(opportunities[0].accepted);
}

fn test_book_record(
    exchange: &str,
    symbol: &str,
    bid: f64,
    ask: f64,
    quantity: f64,
    sequence: u64,
) -> BookRecord {
    BookRecord::BookSnapshot {
        timestamp_local: Utc::now(),
        timestamp_exchange: Some(Utc::now()),
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        bids: vec![OrderBookLevel {
            price: bid,
            quantity,
        }],
        asks: vec![OrderBookLevel {
            price: ask,
            quantity,
        }],
        sequence: Some(sequence),
        source: BookSource::Replay,
        latency_ms: Some(1),
    }
}

fn replay_config_with_paths(input_path: &str, output_path: &str) -> SpotSpotTakerArbitrageConfig {
    let mut cfg = config();
    cfg.market_data_mode = MarketDataMode::Replay;
    cfg.replay.enabled = true;
    cfg.replay.input_path = input_path.to_string();
    cfg.replay.output_path = output_path.to_string();
    cfg.mexc.base_url = "http://127.0.0.1:1".to_string();
    cfg.coinex.base_url = "http://127.0.0.1:1".to_string();
    cfg
}

fn test_opportunity_record(accepted: bool) -> OpportunityRecord {
    OpportunityRecord {
        timestamp: Utc::now(),
        symbol: "CUDISUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_price: 1.0,
        sell_price: 1.03,
        raw_spread_bps: 300.0,
        buy_fee_bps: 10.0,
        sell_fee_bps: 10.0,
        fee_source_buy: FeeSource::ConfigDefault,
        fee_source_sell: FeeSource::ConfigDefault,
        platform_discount_applied: false,
        estimated_fee_bps: 20.0,
        estimated_slippage_bps: 2.0,
        safety_buffer_bps: 3.0,
        estimated_net_spread_bps: 25.0,
        estimated_total_fee: 0.1,
        estimated_gross_pnl: 1.5,
        estimated_net_pnl: 1.4,
        executable_notional: 50.0,
        quantity: 50.0,
        accepted,
        rejection_reason: (!accepted).then_some(RejectionReason::NetSpreadBelowThreshold),
        rejection_detail: None,
        buy_book_age_ms: 1,
        sell_book_age_ms: 1,
        buy_book_source: BookSource::Replay,
        sell_book_source: BookSource::Replay,
        buy_latency_ms: Some(1),
        sell_latency_ms: Some(1),
    }
}
