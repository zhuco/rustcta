use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use rustcta_strategy_sdk::{
    AccountEvent, ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
    ExecutionOrderAck, ExecutionOrderCommand, MarketDataEvent, MarketType, SdkResult,
    StrategyCommand, StrategyContext, StrategyEvent, StrategyExecutionClient, StrategyInstanceId,
    StrategyRuntime,
};
use rustcta_strategy_unified_arbitrage::{
    build_route_position_snapshot, build_split_plan, evaluate_hedge_residual,
    evaluate_route_opportunity, unified_market_data_subscriptions, validate_manual_close,
    validate_manual_open, CanonicalSymbol, CloseScope, ExecutionStyle, FundingSnapshot,
    HedgeStatus, InstrumentKey, LegPositionSnapshot, ManualCloseCommand, ManualOpenCommand,
    OrderBookTop, OrderSide, PositionSide, PositionSidePolicy, RouteMarketInput,
    SplitExecutionConfig, UnifiedArbitrageConfig, UnifiedArbitrageRuntime,
};

fn load_config() -> UnifiedArbitrageConfig {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/unified_arbitrage_usdt.yml"
    );
    let raw = std::fs::read_to_string(path).expect("read unified config");
    serde_yaml::from_str(&raw).expect("parse unified config")
}

#[test]
fn unified_config_should_parse_and_validate_all_route_kinds() {
    let config = load_config();
    config.validate().expect("valid unified config");

    assert_eq!(config.strategy_kind, "unified_arbitrage");
    assert_eq!(config.routes.len(), 3);
    assert!(config
        .routes
        .iter()
        .any(|route| route.kind.to_string() == "spot_perp_basis"));
    assert!(config
        .routes
        .iter()
        .any(|route| route.kind.to_string() == "perp_perp_spread"));
    assert!(config
        .routes
        .iter()
        .any(|route| route.kind.to_string() == "single_exchange_settlement"));
}

#[test]
fn market_data_subscriptions_should_keep_spot_and_perp_separate() {
    let config = load_config();
    let subscriptions = unified_market_data_subscriptions(&config);

    assert!(subscriptions.iter().any(|subscription| {
        subscription.exchange_id == "gate"
            && subscription.symbol == "ORDI/USDT"
            && subscription.market_type == MarketType::Spot
    }));
    assert!(subscriptions.iter().any(|subscription| {
        subscription.exchange_id == "bitget"
            && subscription.symbol == "ORDI/USDT"
            && subscription.market_type == MarketType::Perpetual
    }));
}

#[test]
fn perp_perp_route_should_show_funding_diff_as_edge() {
    let config = load_config();
    let route = config
        .routes
        .iter()
        .find(|route| route.route_id == "drift_binance_bitget_perp")
        .expect("perp route");
    let now = Utc::now();
    let symbol = CanonicalSymbol::parse("DRIFT/USDT").unwrap();
    let mut books = BTreeMap::new();
    books.insert(
        "long".to_string(),
        book(
            "binance",
            MarketType::Perpetual,
            symbol.clone(),
            1.00,
            1.01,
            now,
        ),
    );
    books.insert(
        "short".to_string(),
        book(
            "bitget",
            MarketType::Perpetual,
            symbol.clone(),
            1.04,
            1.05,
            now,
        ),
    );
    let mut funding_snapshots = BTreeMap::new();
    funding_snapshots.insert(
        "long".to_string(),
        funding("binance", symbol.clone(), -0.0001, now),
    );
    funding_snapshots.insert(
        "short".to_string(),
        funding("bitget", symbol.clone(), 0.0004, now),
    );
    let input = RouteMarketInput {
        route_id: route.route_id.clone(),
        books,
        funding: funding_snapshots,
        fees: BTreeMap::new(),
        observed_at: now,
    };

    let opportunity = evaluate_route_opportunity(route, &config.defaults, &input);

    assert!(opportunity.funding_edge_bps > 4.9);
    assert!(opportunity.spread_bps > 200.0);
    assert!(opportunity.expected_funding_pnl_usdt > 0.0);
}

#[test]
fn settlement_policy_should_choose_side_that_receives_funding() {
    assert_eq!(
        PositionSidePolicy::ReceiveFunding.position_side(0.0005),
        PositionSide::Short
    );
    assert_eq!(
        PositionSidePolicy::ReceiveFunding.open_side(-0.0005),
        OrderSide::Buy
    );
}

#[test]
fn split_plan_should_bound_child_size_and_delay_children() {
    let plan = build_split_plan(
        95.0,
        SplitExecutionConfig {
            max_child_notional_usdt: 20.0,
            max_child_orders: 10,
            child_delay_ms: 300,
            ..SplitExecutionConfig::default()
        },
    );

    assert!(plan.enabled);
    assert_eq!(plan.child_count, 5);
    assert!(plan.child_notional_usdt <= 20.0);
    assert_eq!(plan.child_delay_ms, 300);
    assert!(plan.reprice_before_each_child);
}

#[test]
fn hedge_residual_should_repair_without_global_stop() {
    let config = load_config();
    let hedge = evaluate_hedge_residual(10.0, 8.0, 1.5, config.defaults.hedge_tolerance);

    assert_eq!(hedge.residual_notional_usdt, 3.0);
    assert_eq!(hedge.status, HedgeStatus::Repairing);
}

#[test]
fn route_position_snapshot_should_include_pnl_cost_quantity_and_liquidation_distance() {
    let config = load_config();
    let route = config
        .routes
        .iter()
        .find(|route| route.route_id == "drift_binance_bitget_perp")
        .unwrap();
    let opportunity = rustcta_strategy_unified_arbitrage::RouteOpportunity {
        route_id: route.route_id.clone(),
        kind: route.kind,
        symbol: route.symbol.clone(),
        spread_bps: 30.0,
        funding_edge_bps: 5.0,
        expected_net_edge_bps: 10.0,
        expected_funding_pnl_usdt: 0.05,
        accepted: true,
        reject_reason: None,
        open_style: ExecutionStyle::DualTaker,
        close_style: ExecutionStyle::DualTakerReduceOnly,
        split_plan: build_split_plan(10.0, config.defaults.split_execution),
    };
    let legs = vec![
        leg_snapshot("long", "binance", PositionSide::Long, 10.0, 1.0, 1.02, 25.0),
        leg_snapshot(
            "short",
            "bitget",
            PositionSide::Short,
            10.0,
            1.04,
            1.01,
            35.0,
        ),
    ];

    let snapshot =
        build_route_position_snapshot(route, &opportunity, legs, config.defaults.hedge_tolerance);

    assert!(snapshot.position_value_usdt > 0.0);
    assert!(snapshot.net_pnl_usdt > 0.0);
    assert!(snapshot.quantity_base > 0.0);
    assert_eq!(snapshot.nearest_liquidation_distance_pct, Some(25.0));
    assert_eq!(snapshot.hedge.status, HedgeStatus::Matched);
}

#[test]
fn manual_open_and_close_commands_should_validate_control_surface() {
    let config = load_config();
    let open = ManualOpenCommand {
        route_id: "drift_binance_bitget_perp".to_string(),
        notional_usdt: 10.0,
        execution_style: Some(ExecutionStyle::DualTaker),
        use_split_execution: true,
        max_slippage_bps: 10.0,
        require_current_signal: false,
        dry_run_preview: true,
        operator_confirmation_id: Some("confirm-1".to_string()),
    };
    assert!(validate_manual_open(&open, &config, None).accepted);

    let close = ManualCloseCommand {
        scope: CloseScope::Route,
        route_id: Some("drift_binance_bitget_perp".to_string()),
        bundle_id: None,
        symbol: None,
        reason: "operator_request".to_string(),
        execution_style: Some(ExecutionStyle::DualTakerReduceOnly),
        reduce_only: true,
        close_residual_only: false,
        max_slippage_bps: 15.0,
        dry_run_preview: true,
        operator_confirmation_id: Some("confirm-2".to_string()),
    };
    assert!(validate_manual_close(&close, &config).accepted);
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_should_update_market_positions_and_submit_owned_manual_intents() {
    let config = load_config();
    let config_value = serde_json::to_value(&config).unwrap();
    let execution = Arc::new(MockExecutionClient::default());
    let mut runtime = UnifiedArbitrageRuntime::new();
    runtime
        .start(StrategyContext::new(
            StrategyInstanceId::new("unified_arb_live:test"),
            "local",
            "unified_arbitrage",
            "unified_arb_live",
            "test",
            config_value,
            execution.clone(),
        ))
        .await
        .expect("runtime starts");

    let route = config
        .routes
        .iter()
        .find(|route| route.route_id == "drift_binance_bitget_perp")
        .unwrap();
    let now = Utc::now();
    let symbol = CanonicalSymbol::parse("DRIFT/USDT").unwrap();
    let input = RouteMarketInput {
        route_id: route.route_id.clone(),
        books: BTreeMap::from([
            (
                "long".to_string(),
                book(
                    "binance",
                    MarketType::Perpetual,
                    symbol.clone(),
                    1.00,
                    1.01,
                    now,
                ),
            ),
            (
                "short".to_string(),
                book(
                    "bitget",
                    MarketType::Perpetual,
                    symbol.clone(),
                    1.04,
                    1.05,
                    now,
                ),
            ),
        ]),
        funding: BTreeMap::from([
            (
                "long".to_string(),
                funding("binance", symbol.clone(), -0.0001, now),
            ),
            (
                "short".to_string(),
                funding("bitget", symbol.clone(), 0.0004, now),
            ),
        ]),
        fees: BTreeMap::new(),
        observed_at: now,
    };
    runtime
        .handle_event(StrategyEvent::MarketData(MarketDataEvent {
            schema_version: 1,
            exchange_id: "synthetic".to_string(),
            symbol: "DRIFT/USDT".to_string(),
            received_at: now,
            payload: serde_json::to_value(input).unwrap(),
        }))
        .await
        .expect("market data handled");

    let opportunity = rustcta_strategy_unified_arbitrage::RouteOpportunity {
        route_id: route.route_id.clone(),
        kind: route.kind,
        symbol: route.symbol.clone(),
        spread_bps: 30.0,
        funding_edge_bps: 5.0,
        expected_net_edge_bps: 10.0,
        expected_funding_pnl_usdt: 0.05,
        accepted: true,
        reject_reason: None,
        open_style: ExecutionStyle::DualTaker,
        close_style: ExecutionStyle::DualTakerReduceOnly,
        split_plan: build_split_plan(10.0, config.defaults.split_execution),
    };
    let position = build_route_position_snapshot(
        route,
        &opportunity,
        vec![
            leg_snapshot("long", "binance", PositionSide::Long, 10.0, 1.0, 1.02, 25.0),
            leg_snapshot(
                "short",
                "bitget",
                PositionSide::Short,
                10.0,
                1.04,
                1.01,
                35.0,
            ),
        ],
        config.defaults.hedge_tolerance,
    );
    runtime
        .handle_event(StrategyEvent::Account(AccountEvent {
            schema_version: 1,
            account_id: "unified_arbitrage".to_string(),
            received_at: now,
            payload: serde_json::to_value(position).unwrap(),
        }))
        .await
        .expect("account snapshot handled");

    runtime
        .handle_event(StrategyEvent::OperatorCommand(StrategyCommand {
            schema_version: 1,
            command_id: "cmd-open-1".to_string(),
            instance_id: StrategyInstanceId::new("unified_arb_live:test"),
            command_kind: "manual_open_route".to_string(),
            requested_at: now,
            payload: serde_json::json!({
                "route_id": "drift_binance_bitget_perp",
                "notional_usdt": 20.0,
                "execution_style": "dual_taker",
                "use_split_execution": true,
                "max_slippage_bps": 10.0,
                "require_current_signal": true,
                "dry_run_preview": false,
                "operator_confirmation_id": "confirm-open"
            }),
            requested_by: Some("test".to_string()),
        }))
        .await
        .expect("manual open handled");

    let snapshot = runtime.snapshot().await.expect("snapshot");
    assert_eq!(
        snapshot.payload["last_opportunities"][0]["route_id"],
        "drift_binance_bitget_perp"
    );
    assert!(snapshot.payload["position_value_usdt"].as_f64().unwrap() > 0.0);
    assert_eq!(snapshot.payload["nearest_liquidation_distance_pct"], 25.0);
    assert_eq!(snapshot.payload["submitted_execution_intents"], 1);

    let intents = execution.intents.lock().expect("intents mutex").clone();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].intent_kind, "unified_arbitrage_open_route");
    assert_eq!(
        intents[0].payload["owned_position_scope"]["route_id"],
        "drift_binance_bitget_perp"
    );
    assert_eq!(intents[0].payload["split_plan"]["child_count"], 1);
}

fn book(
    exchange: &str,
    market_type: MarketType,
    symbol: CanonicalSymbol,
    bid: f64,
    ask: f64,
    now: chrono::DateTime<Utc>,
) -> OrderBookTop {
    OrderBookTop {
        instrument: InstrumentKey::new(exchange, market_type, symbol),
        best_bid_price: bid,
        best_bid_quantity: 100.0,
        best_ask_price: ask,
        best_ask_quantity: 100.0,
        levels: 5,
        received_at: now,
    }
}

fn funding(
    exchange: &str,
    symbol: CanonicalSymbol,
    funding_rate: f64,
    now: chrono::DateTime<Utc>,
) -> FundingSnapshot {
    FundingSnapshot {
        instrument: InstrumentKey::new(exchange, MarketType::Perpetual, symbol),
        funding_rate,
        predicted_funding_rate: None,
        funding_interval_hours: 8.0,
        next_funding_time: Some(now + chrono::Duration::hours(1)),
        updated_at: now,
    }
}

fn leg_snapshot(
    leg_id: &str,
    exchange: &str,
    position_side: PositionSide,
    quantity: f64,
    entry: f64,
    mark: f64,
    liq_distance: f64,
) -> LegPositionSnapshot {
    let sign = match position_side {
        PositionSide::Long => 1.0,
        PositionSide::Short => -1.0,
        PositionSide::None => 0.0,
    };
    LegPositionSnapshot {
        leg_id: leg_id.to_string(),
        exchange: exchange.to_string(),
        market_type: MarketType::Perpetual,
        position_side,
        leverage: Some(5),
        quantity_base: quantity,
        quantity_contracts: quantity,
        position_value_usdt: quantity * mark,
        avg_entry_price: entry,
        mark_price: mark,
        liquidation_price: Some(mark * (1.0 + sign * 0.5)),
        liquidation_distance_pct: Some(liq_distance),
        unrealized_pnl_usdt: sign * quantity * (mark - entry),
        realized_pnl_usdt: 0.0,
        funding_rate: Some(0.0001),
        predicted_funding_rate: None,
        next_funding_time: None,
        margin_used_usdt: Some(quantity * mark / 5.0),
        maintenance_margin_usdt: Some(quantity * mark * 0.005),
    }
}

#[derive(Default)]
struct MockExecutionClient {
    intents: Mutex<Vec<ExecutionIntent>>,
}

#[async_trait]
impl StrategyExecutionClient for MockExecutionClient {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck> {
        Ok(ExecutionOrderAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: None,
            reason: Some("orders are not used by unified runtime contract test".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        Ok(ExecutionCancelAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: command.execution_order_id,
            reason: Some("cancels are not used by unified runtime contract test".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        self.intents
            .lock()
            .expect("intents mutex")
            .push(intent.clone());
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: true,
            intent_kind: intent.intent_kind,
            reason: None,
            received_at: Utc::now(),
            payload: serde_json::json!({ "accepted_by": "mock" }),
        })
    }
}
