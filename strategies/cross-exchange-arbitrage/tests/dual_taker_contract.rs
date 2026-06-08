use chrono::{DateTime, Duration, TimeZone, Utc};
use rustcta_strategy_cross_exchange_arbitrage::core::{QuantityUnit, TakerFillAudit};
use rustcta_strategy_cross_exchange_arbitrage::{
    cross_exchange_market_data_subscriptions,
    cross_exchange_market_data_subscriptions_for_high_volatility, evaluate_dual_taker_close,
    evaluate_dual_taker_open_opportunities, evaluate_ready_dual_taker_open_opportunities,
    inspect_single_leg_net_positions, plan_startup_usdt_position_takeover, ArbitrageRiskState,
    CanonicalSymbol, CloseReason, CrossExchangeArbitrageConfig, DualTakerArbitrageConfig,
    ExchangeId, ExchangeStartupReadiness, ExchangeStatusRegistry, FeeModel, NetPosition,
    OpenArbitragePosition, OpenBlockReason, OrderBookTop, OrderSide, PairedTakerFillState,
    PositionSide, PrecisionRegistry, SingleLegGuard, StartupReadiness, StartupUsdtPosition,
    StrategyLogEventKind, StrategyLogRotationConfig, SymbolPrecision, TakerOrderRole,
    VolatilityRankDirection, VolatilityRankTicker,
};
use rustcta_strategy_sdk::{MarketDataChannel, MarketType};
use serde_json::json;

const EPSILON: f64 = 0.000_000_001;

#[test]
fn config_should_filter_blacklisted_btc_eth_bnb_symbols() {
    let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
        "venues": ["binance", "gate", "bitget"],
        "symbols": [
            "BTC/USDT",
            "eth_usdt",
            "bnb-usdt",
            "SOLUSDT",
            "edge/usdt",
            "DRIFT-USDT"
        ],
        "excluded_bases": ["BTC", "ETH", "BNB"]
    }));

    let active_symbols = config.active_symbols();

    assert_eq!(active_symbols, vec!["SOL/USDT", "EDGE/USDT", "DRIFT/USDT"]);
    assert!(!active_symbols
        .iter()
        .any(|symbol| symbol.starts_with("BTC/")));
    assert!(!active_symbols
        .iter()
        .any(|symbol| symbol.starts_with("ETH/")));
    assert!(!active_symbols
        .iter()
        .any(|symbol| symbol.starts_with("BNB/")));
}

#[test]
fn market_data_subscriptions_should_use_perpetual_depth_and_fastest_custom_l1() {
    let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
        "venues": ["binance", "gate", "bitget"],
        "symbols": ["EDGE/USDT"],
        "market_type": "perpetual"
    }));

    let subscriptions = cross_exchange_market_data_subscriptions(&config);

    assert_eq!(subscriptions.len(), 3);
    for subscription in subscriptions {
        assert_eq!(subscription.symbol, "EDGE/USDT");
        assert_eq!(subscription.market_type, MarketType::Perpetual);
        assert_eq!(
            subscription.channels,
            vec![
                MarketDataChannel::OrderBookDepth,
                MarketDataChannel::Custom("fastest_l1_10ms".to_string()),
            ]
        );
    }
}

#[test]
fn high_volatility_gainers_and_losers_should_extend_orderbook_monitoring() {
    let now = fixed_now();
    let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
        "venues": ["binance", "gate", "bitget"],
        "symbols": ["EDGE/USDT"],
        "excluded_bases": ["BTC", "ETH", "BNB"],
        "volatility_universe": {
            "enabled": true,
            "top_gainers_per_exchange": 1,
            "top_losers_per_exchange": 1,
            "min_abs_change_pct": 0.05,
            "min_quote_volume_usdt": 1000000.0,
            "max_dynamic_symbols": 4,
            "monitor_orderbook": true
        }
    }));
    let tickers = vec![
        rank_ticker(
            "binance",
            "BTC",
            VolatilityRankDirection::Gainer,
            0.22,
            10_000_000.0,
            now,
        ),
        rank_ticker(
            "binance",
            "HYPE",
            VolatilityRankDirection::Gainer,
            0.12,
            5_000_000.0,
            now,
        ),
        rank_ticker(
            "gate",
            "BNB",
            VolatilityRankDirection::Loser,
            -0.30,
            8_000_000.0,
            now,
        ),
        rank_ticker(
            "gate",
            "DRIFT",
            VolatilityRankDirection::Loser,
            -0.14,
            3_000_000.0,
            now,
        ),
        rank_ticker(
            "bitget",
            "ILLQ",
            VolatilityRankDirection::Gainer,
            0.40,
            100_000.0,
            now,
        ),
        rank_ticker(
            "bitget",
            "SLOW",
            VolatilityRankDirection::Loser,
            -0.03,
            9_000_000.0,
            now,
        ),
    ];

    let symbols = config.active_symbols_with_high_volatility(&tickers);
    assert_eq!(symbols, vec!["EDGE/USDT", "HYPE/USDT", "DRIFT/USDT"]);

    let subscriptions =
        cross_exchange_market_data_subscriptions_for_high_volatility(&config, &tickers);
    assert_eq!(subscriptions.len(), 9);
    assert!(subscriptions
        .iter()
        .any(|subscription| subscription.symbol == "HYPE/USDT"));
    assert!(subscriptions
        .iter()
        .any(|subscription| subscription.symbol == "DRIFT/USDT"));
    assert!(!subscriptions
        .iter()
        .any(|subscription| subscription.symbol == "BTC/USDT"));
    assert!(!subscriptions
        .iter()
        .any(|subscription| subscription.symbol == "BNB/USDT"));
}

#[test]
fn startup_readiness_should_block_calculation_until_every_exchange_gate_is_ready() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let books = valid_open_books(&symbol, now);
    let precision = precision_registry(&symbol);
    let config = DualTakerArbitrageConfig::default();
    let fee_model = FeeModel::default();
    let not_ready = StartupReadiness {
        exchanges: vec![
            ready_exchange("binance", 1),
            ExchangeStartupReadiness {
                exchange: ExchangeId::new("gate"),
                expected_symbols: 2,
                market_data_subscribed_symbols: 1,
                user_stream_subscribed: false,
                server_time_synced: false,
                symbol_rules_loaded: false,
            },
        ],
    };

    assert!(!not_ready.can_evaluate_opportunities());
    assert_eq!(
        not_ready.blocked_reasons()["gate"],
        vec![
            "market_data_subscription",
            "user_stream_subscription",
            "server_time_sync",
            "symbol_rules"
        ]
    );

    let gated_opportunities = evaluate_ready_dual_taker_open_opportunities(
        &not_ready, &books, &precision, &fee_model, &config, now,
    );

    assert!(
        gated_opportunities.is_empty(),
        "opportunity evaluation must be skipped until all startup gates are ready"
    );
    assert!(
        !evaluate_dual_taker_open_opportunities(&books, &precision, &fee_model, &config, now)
            .is_empty(),
        "test books should contain an opportunity once the readiness gate opens"
    );

    let ready = StartupReadiness {
        exchanges: vec![ready_exchange("binance", 1), ready_exchange("gate", 1)],
    };
    assert!(ready.can_evaluate_opportunities());
    assert_eq!(
        evaluate_ready_dual_taker_open_opportunities(
            &ready, &books, &precision, &fee_model, &config, now
        )
        .len(),
        1
    );
}

#[test]
fn dual_taker_open_should_use_one_shared_quantity_and_configured_spread_window() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let precision = precision_registry(&symbol);
    let fee_model = FeeModel::default();
    let config = DualTakerArbitrageConfig {
        target_notional_usdt: 5.5,
        min_open_spread_pct: 0.005,
        max_open_spread_pct: 0.05,
        taker_slippage_pct: 0.0005,
        ..DualTakerArbitrageConfig::default()
    };

    let opportunities = evaluate_dual_taker_open_opportunities(
        &valid_open_books(&symbol, now),
        &precision,
        &fee_model,
        &config,
        now,
    );

    assert_eq!(opportunities.len(), 1);
    let opportunity = &opportunities[0];
    assert_eq!(opportunity.long_exchange, ExchangeId::new("binance"));
    assert_eq!(opportunity.short_exchange, ExchangeId::new("gate"));
    assert_eq!(opportunity.long_entry_price, 100.0);
    assert_eq!(opportunity.short_entry_price, 100.6);
    assert!((opportunity.spread_pct - 0.006).abs() < EPSILON);
    assert!(opportunity.spread_pct >= config.min_open_spread_pct);
    assert!(opportunity.spread_pct <= config.max_open_spread_pct);
    assert_eq!(opportunity.top_of_book_capacity_ratio, 0.8);
    assert_eq!(opportunity.expected_close_spread_pct, 0.001);
    assert!(
        (opportunity.expected_gross_pnl_usdt
            - opportunity.quantity * opportunity.long_entry_price * 0.005)
            .abs()
            < EPSILON
    );
    assert!(
        (opportunity.estimated_round_trip_fee_usdt
            - (opportunity.long_notional_usdt + opportunity.short_notional_usdt) * 0.001)
            .abs()
            < EPSILON,
        "expected model must deduct four taker fees"
    );
    assert!(
        (opportunity.expected_net_pnl_usdt
            - (opportunity.expected_gross_pnl_usdt - opportunity.estimated_round_trip_fee_usdt))
            .abs()
            < EPSILON
    );
    assert!(opportunity.expected_net_profit_pct > 0.0);
    assert!(opportunity.submit_parallel);
    assert_eq!(opportunity.orders.len(), 2);
    assert!(
        opportunity
            .orders
            .iter()
            .all(|order| (order.base_quantity - opportunity.quantity).abs() < EPSILON),
        "both taker legs must share the same base quantity"
    );
    assert!(
        (opportunity.long_notional_usdt - opportunity.short_notional_usdt).abs() > EPSILON,
        "same quantity is required even when leg notionals differ by venue price"
    );
    assert_eq!(opportunity.orders[0].side, OrderSide::Buy);
    assert_eq!(opportunity.orders[0].role, TakerOrderRole::OpenLong);
    assert!(!opportunity.orders[0].reduce_only);
    assert_eq!(opportunity.orders[1].side, OrderSide::Sell);
    assert_eq!(opportunity.orders[1].role, TakerOrderRole::OpenShort);
    assert!(!opportunity.orders[1].reduce_only);

    let below_min = vec![
        book("binance", &symbol, 99.9, 100.0, 20.0, 20.0, now),
        book("gate", &symbol, 100.39, 100.5, 20.0, 20.0, now),
    ];
    assert!(evaluate_dual_taker_open_opportunities(
        &below_min, &precision, &fee_model, &config, now
    )
    .is_empty());

    let above_max = vec![
        book("binance", &symbol, 99.9, 100.0, 20.0, 20.0, now),
        book("gate", &symbol, 105.1, 105.2, 20.0, 20.0, now),
    ];
    assert!(evaluate_dual_taker_open_opportunities(
        &above_max, &precision, &fee_model, &config, now
    )
    .is_empty());
}

#[test]
fn open_quantity_should_use_top_depth_usdt_capacity_and_skip_thin_books() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let precision = precision_registry(&symbol);
    let fee_model = FeeModel::default();
    let config = DualTakerArbitrageConfig {
        target_notional_usdt: 5.5,
        top_of_book_capacity_ratio: 0.8,
        ..DualTakerArbitrageConfig::default()
    };
    let books = vec![
        book("binance", &symbol, 99.9, 100.0, 0.2, 20.0 / 100.0, now),
        book("gate", &symbol, 100.6, 100.7, 15.0 / 100.6, 0.2, now),
    ];

    let opportunities =
        evaluate_dual_taker_open_opportunities(&books, &precision, &fee_model, &config, now);

    assert_eq!(opportunities.len(), 1);
    assert!((opportunities[0].executable_top_depth_usdt - 12.0).abs() < EPSILON);
    assert!(opportunities[0].long_notional_usdt >= config.target_notional_usdt);
    assert!(opportunities[0].short_notional_usdt >= config.target_notional_usdt);

    let thin_books = vec![
        book("binance", &symbol, 99.9, 100.0, 0.2, 20.0 / 100.0, now),
        book("gate", &symbol, 100.6, 100.7, 6.0 / 100.6, 0.2, now),
    ];
    assert!(
        evaluate_dual_taker_open_opportunities(&thin_books, &precision, &fee_model, &config, now)
            .is_empty(),
        "6 USDT top depth * 80% is below the 5.5 USDT target"
    );
}

#[test]
fn gate_contract_units_should_keep_filled_contracts_and_audit_base_costs() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let mut precision = PrecisionRegistry::default();
    precision.insert(
        ExchangeId::new("binance"),
        symbol.clone(),
        SymbolPrecision {
            price_tick: 0.01,
            quantity_step: 0.001,
            min_quantity: 0.001,
            min_notional_usdt: 0.0,
            ..SymbolPrecision::default()
        },
    );
    precision.insert(
        ExchangeId::new("gate"),
        symbol.clone(),
        SymbolPrecision {
            price_tick: 0.01,
            quantity_step: 1.0,
            min_quantity: 1.0,
            min_notional_usdt: 0.0,
            quantity_unit: QuantityUnit::Contracts,
            contract_size: 0.001,
        },
    );
    let books = vec![
        book("binance", &symbol, 99.9, 100.0, 0.2, 0.2, now),
        book("gate", &symbol, 100.6, 100.7, 1_000.0, 1_000.0, now),
    ];

    let opportunity = evaluate_dual_taker_open_opportunities(
        &books,
        &precision,
        &FeeModel::default(),
        &DualTakerArbitrageConfig::default(),
        now,
    )
    .pop()
    .expect("contract-sized gate book should still produce an opportunity");

    assert!((opportunity.quantity - 0.055).abs() < EPSILON);
    let long_order = &opportunity.orders[0];
    let short_order = &opportunity.orders[1];
    assert_eq!(long_order.quantity_unit, QuantityUnit::Base);
    assert!((long_order.quantity - 0.055).abs() < EPSILON);
    assert_eq!(short_order.quantity_unit, QuantityUnit::Contracts);
    assert_eq!(short_order.quantity, 55.0);
    assert!((short_order.base_quantity - 0.055).abs() < EPSILON);

    let audit = TakerFillAudit::from_order_fill(
        "bundle-gate",
        short_order,
        55.0,
        100.55,
        FeeModel::default().rate(
            &ExchangeId::new("gate"),
            rustcta_strategy_cross_exchange_arbitrage::FeeRole::Taker,
        ),
    );
    assert_eq!(audit.actual_order_quantity, 55.0);
    assert_eq!(audit.quantity_unit, QuantityUnit::Contracts);
    assert!((audit.actual_base_quantity - 0.055).abs() < EPSILON);
    assert!((audit.planned_price - 100.6).abs() < EPSILON);
    assert!((audit.actual_fill_price - 100.55).abs() < EPSILON);
    assert!((audit.actual_notional_usdt - 0.055 * 100.55).abs() < EPSILON);
    assert!(audit.slippage_pct > 0.0);
}

#[test]
fn orderbook_older_than_500ms_should_be_rejected() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let precision = precision_registry(&symbol);
    let fee_model = FeeModel::default();
    let config = DualTakerArbitrageConfig {
        orderbook_stale_ms: 500,
        ..DualTakerArbitrageConfig::default()
    };
    let books = vec![
        book(
            "binance",
            &symbol,
            99.9,
            100.0,
            20.0,
            20.0,
            now - Duration::milliseconds(501),
        ),
        book("gate", &symbol, 100.6, 100.7, 20.0, 20.0, now),
    ];

    assert!(
        evaluate_dual_taker_open_opportunities(&books, &precision, &fee_model, &config, now)
            .is_empty()
    );
}

#[test]
fn single_leg_guard_should_emergency_reduce_only_close_after_600ms_and_stop_after_three() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let config = DualTakerArbitrageConfig {
        single_leg_timeout_ms: 600,
        max_consecutive_single_leg_fills: 3,
        taker_slippage_pct: 0.0005,
        ..DualTakerArbitrageConfig::default()
    };
    let precision = SymbolPrecision {
        price_tick: 0.01,
        quantity_step: 0.001,
        min_quantity: 0.001,
        min_notional_usdt: 0.0,
        quantity_unit: QuantityUnit::Base,
        contract_size: 1.0,
    };
    let mut guard = SingleLegGuard::default();

    let pending = guard.evaluate(
        &paired_fill_state("bundle-pending", &symbol, now, true, false),
        &config,
        100.0,
        precision,
        now + Duration::milliseconds(599),
    );
    assert!(!pending.emergency_close_required);
    assert!(!pending.stop_strategy);
    assert!(pending.close_order.is_none());

    for attempt in 1..=3 {
        let decision = guard.evaluate(
            &paired_fill_state(&format!("bundle-{attempt}"), &symbol, now, true, false),
            &config,
            100.0,
            precision,
            now + Duration::milliseconds(600),
        );
        let close_order = decision
            .close_order
            .expect("single filled long leg must produce an emergency close order");

        assert!(decision.emergency_close_required);
        assert_eq!(decision.consecutive_single_leg_fills, attempt);
        assert_eq!(close_order.exchange, ExchangeId::new("binance"));
        assert_eq!(close_order.side, OrderSide::Sell);
        assert_eq!(close_order.quantity, 0.054);
        assert!(close_order.reduce_only);
        assert_eq!(close_order.role, TakerOrderRole::EmergencyCloseLong);
        assert_eq!(decision.stop_strategy, attempt == 3);
        assert!(decision.warning.is_some());
    }
}

#[test]
fn close_evaluation_should_require_net_profit_after_four_taker_fees() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let precision = precision_registry(&symbol);
    let fee_model = FeeModel::default();
    let config = DualTakerArbitrageConfig {
        close_min_net_profit_pct: 0.0005,
        ..DualTakerArbitrageConfig::default()
    };
    let position = OpenArbitragePosition {
        bundle_id: "bundle-close".to_string(),
        canonical_symbol: symbol.clone(),
        long_exchange: ExchangeId::new("binance"),
        short_exchange: ExchangeId::new("gate"),
        quantity: 0.054,
        long_entry_price: 100.0,
        short_entry_price: 100.6,
        opened_at: now - Duration::minutes(10),
    };
    let long_book = book("binance", &symbol, 100.5, 100.6, 20.0, 20.0, now);
    let short_book = book("gate", &symbol, 99.9, 100.0, 20.0, 20.0, now);

    let close = evaluate_dual_taker_close(
        &position,
        &long_book,
        &short_book,
        &precision,
        &fee_model,
        &config,
        now,
    )
    .expect("fresh close books should evaluate");

    assert!(close.should_close);
    assert_eq!(close.reason, Some(CloseReason::ProfitTarget));
    assert!(close.gross_pnl_usdt > close.total_fee_usdt);
    assert!(close.net_profit_pct > config.close_min_net_profit_pct);
    assert_eq!(close.orders.len(), 2);
    assert!(close.orders.iter().all(|order| order.reduce_only));
    assert_eq!(close.orders[0].role, TakerOrderRole::CloseLong);
    assert_eq!(close.orders[1].role, TakerOrderRole::CloseShort);
}

#[test]
fn risk_state_should_limit_symbol_concurrency_cooldown_and_exchange_positions() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let config = DualTakerArbitrageConfig::default();
    let mut risk = ArbitrageRiskState::default();

    risk.record_open(open_position("bundle-edge", symbol.clone(), now));
    assert_eq!(
        risk.can_open(
            &symbol,
            &ExchangeId::new("binance"),
            &ExchangeId::new("gate"),
            &config,
            now,
        ),
        Err(OpenBlockReason::SymbolAlreadyActive)
    );

    risk.record_close("bundle-edge", now, config.symbol_cooldown_secs);
    assert_eq!(
        risk.can_open(
            &symbol,
            &ExchangeId::new("binance"),
            &ExchangeId::new("gate"),
            &config,
            now + Duration::seconds(299),
        ),
        Err(OpenBlockReason::SymbolCoolingDown)
    );

    let mut saturated = ArbitrageRiskState::default();
    for index in 0..config.max_positions_per_exchange {
        saturated.record_open(open_position(
            &format!("bundle-{index}"),
            CanonicalSymbol::new(format!("EDGE{index}"), "USDT"),
            now,
        ));
    }
    assert_eq!(
        saturated.can_open(
            &CanonicalSymbol::new("NEXT", "USDT"),
            &ExchangeId::new("binance"),
            &ExchangeId::new("bitget"),
            &config,
            now,
        ),
        Err(OpenBlockReason::ExchangePositionLimit)
    );
}

#[test]
fn net_position_inspection_should_warn_on_single_leg_exposure_only() {
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let balanced = CanonicalSymbol::new("DRIFT", "USDT");
    let warnings = inspect_single_leg_net_positions(
        &[
            NetPosition {
                exchange: ExchangeId::new("binance"),
                canonical_symbol: symbol.clone(),
                quantity: 0.054,
            },
            NetPosition {
                exchange: ExchangeId::new("binance"),
                canonical_symbol: balanced.clone(),
                quantity: 0.1,
            },
            NetPosition {
                exchange: ExchangeId::new("gate"),
                canonical_symbol: balanced,
                quantity: -0.1,
            },
        ],
        0.000001,
    );

    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].canonical_symbol, symbol);
    assert!(warnings[0].message.contains("single leg"));
}

#[test]
fn startup_takeover_should_adopt_balanced_positions_and_close_single_legs() {
    let now = fixed_now();
    let symbol = CanonicalSymbol::new("EDGE", "USDT");
    let solo = CanonicalSymbol::new("SOLO", "USDT");
    let mut precision = precision_registry(&symbol);
    precision.insert(
        ExchangeId::new("bitget"),
        solo.clone(),
        SymbolPrecision {
            price_tick: 0.01,
            quantity_step: 0.001,
            min_quantity: 0.001,
            min_notional_usdt: 0.0,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
        },
    );
    let config = DualTakerArbitrageConfig::default();
    let plan = plan_startup_usdt_position_takeover(
        &[
            startup_position("binance", &symbol, PositionSide::Long, 0.054, 100.0, now),
            startup_position("gate", &symbol, PositionSide::Short, 0.054, 100.6, now),
            startup_position("bitget", &solo, PositionSide::Long, 0.11, 50.0, now),
        ],
        &[book("bitget", &solo, 49.9, 50.0, 10.0, 10.0, now)],
        &precision,
        &config,
        now,
    );

    assert_eq!(plan.adopted_positions.len(), 1);
    assert_eq!(
        plan.adopted_positions[0].long_exchange,
        ExchangeId::new("binance")
    );
    assert_eq!(
        plan.adopted_positions[0].short_exchange,
        ExchangeId::new("gate")
    );
    assert!(plan.requires_emergency_close());
    assert!(!plan.ready_for_new_opens());
    assert_eq!(plan.single_leg_resolutions.len(), 1);
    let close_order = plan.single_leg_resolutions[0]
        .close_order
        .as_ref()
        .expect("fresh single leg book should produce a close order");
    assert_eq!(close_order.exchange, ExchangeId::new("bitget"));
    assert_eq!(close_order.side, OrderSide::Sell);
    assert!(close_order.reduce_only);
    assert_eq!(close_order.role, TakerOrderRole::EmergencyCloseLong);
}

#[test]
fn exchange_status_registry_should_track_messages_latency_time_offset_and_disconnects() {
    let now = fixed_now();
    let mut statuses = ExchangeStatusRegistry::default();
    let status = statuses.status_mut(ExchangeId::new("binance"));

    status.set_subscribed_symbols(400);
    status.record_message(Some(12), Some(-8), now);
    status.record_message(Some(25), None, now + Duration::milliseconds(10));
    status.record_disconnect();

    let rows = statuses.statuses();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subscribed_symbols, 400);
    assert_eq!(rows[0].message_count, 2);
    assert_eq!(rows[0].last_latency_ms, Some(25));
    assert_eq!(rows[0].max_latency_ms, Some(25));
    assert_eq!(rows[0].server_time_offset_ms, Some(-8));
    assert_eq!(rows[0].disconnect_count, 1);
}

#[test]
fn log_rotation_should_default_to_100mb_five_files_and_skip_heartbeat_in_key_only_mode() {
    let config = StrategyLogRotationConfig::default();

    assert!(config.persist_only_key_events);
    assert_eq!(config.max_file_bytes, 100 * 1024 * 1024);
    assert_eq!(config.retained_files, 5);
    assert!(!config.should_persist(StrategyLogEventKind::MarketDataHeartbeat));
    assert!(config.should_persist(StrategyLogEventKind::StartupReady));
    assert!(config.should_persist(StrategyLogEventKind::OpportunityOpened));

    let all_events = StrategyLogRotationConfig {
        persist_only_key_events: false,
        ..config
    };
    assert!(all_events.should_persist(StrategyLogEventKind::MarketDataHeartbeat));
}

fn fixed_now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("fixed timestamp should be valid")
}

fn ready_exchange(exchange: &str, expected_symbols: usize) -> ExchangeStartupReadiness {
    ExchangeStartupReadiness {
        exchange: ExchangeId::new(exchange),
        expected_symbols,
        market_data_subscribed_symbols: expected_symbols,
        user_stream_subscribed: true,
        server_time_synced: true,
        symbol_rules_loaded: true,
    }
}

fn rank_ticker(
    exchange: &str,
    base: &str,
    direction: VolatilityRankDirection,
    change_pct: f64,
    quote_volume_usdt: f64,
    observed_at: DateTime<Utc>,
) -> VolatilityRankTicker {
    VolatilityRankTicker {
        exchange: ExchangeId::new(exchange),
        canonical_symbol: CanonicalSymbol::new(base, "USDT"),
        direction,
        change_pct,
        quote_volume_usdt,
        observed_at,
    }
}

fn startup_position(
    exchange: &str,
    symbol: &CanonicalSymbol,
    position_side: PositionSide,
    base_quantity: f64,
    entry_price: f64,
    opened_at: DateTime<Utc>,
) -> StartupUsdtPosition {
    StartupUsdtPosition {
        exchange: ExchangeId::new(exchange),
        canonical_symbol: symbol.clone(),
        position_side,
        base_quantity,
        entry_price,
        opened_at,
    }
}

fn valid_open_books(symbol: &CanonicalSymbol, now: DateTime<Utc>) -> Vec<OrderBookTop> {
    vec![
        book("binance", symbol, 99.9, 100.0, 20.0, 20.0, now),
        book("gate", symbol, 100.6, 100.7, 20.0, 20.0, now),
    ]
}

fn precision_registry(symbol: &CanonicalSymbol) -> PrecisionRegistry {
    let mut precision = PrecisionRegistry::default();
    for exchange in ["binance", "gate", "bitget"] {
        precision.insert(
            ExchangeId::new(exchange),
            symbol.clone(),
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 0.001,
                min_quantity: 0.001,
                min_notional_usdt: 0.0,
                quantity_unit: QuantityUnit::Base,
                contract_size: 1.0,
            },
        );
    }
    precision
}

fn book(
    exchange: &str,
    symbol: &CanonicalSymbol,
    best_bid_price: f64,
    best_ask_price: f64,
    best_bid_quantity: f64,
    best_ask_quantity: f64,
    received_at: DateTime<Utc>,
) -> OrderBookTop {
    OrderBookTop {
        exchange: ExchangeId::new(exchange),
        canonical_symbol: symbol.clone(),
        best_bid_price,
        best_bid_quantity,
        best_ask_price,
        best_ask_quantity,
        levels: 1,
        exchange_timestamp: Some(received_at - Duration::milliseconds(10)),
        received_at,
        latency_ms: Some(10),
    }
}

fn paired_fill_state(
    bundle_id: &str,
    symbol: &CanonicalSymbol,
    submitted_at: DateTime<Utc>,
    long_filled: bool,
    short_filled: bool,
) -> PairedTakerFillState {
    PairedTakerFillState {
        bundle_id: bundle_id.to_string(),
        canonical_symbol: symbol.clone(),
        long_exchange: ExchangeId::new("binance"),
        short_exchange: ExchangeId::new("gate"),
        quantity: 0.054,
        submitted_at,
        long_filled,
        short_filled,
    }
}

fn open_position(
    bundle_id: &str,
    symbol: CanonicalSymbol,
    opened_at: DateTime<Utc>,
) -> OpenArbitragePosition {
    OpenArbitragePosition {
        bundle_id: bundle_id.to_string(),
        canonical_symbol: symbol,
        long_exchange: ExchangeId::new("binance"),
        short_exchange: ExchangeId::new("gate"),
        quantity: 0.054,
        long_entry_price: 100.0,
        short_entry_price: 100.6,
        opened_at,
    }
}
