use chrono::Utc;
use rustcta::exchanges::unified::{
    ExchangeClientError, ExchangeError, ExchangeErrorClass, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderRequest, OrderType, SymbolRule, SymbolStatus, TimeInForce,
};
use rustcta::execution::{FeeConfig, FeeModel, FeePairConfig};
use rustcta::risk::{
    classify_market_regime, recommend_hedge, HedgePolicyMode, HedgeVenueCapability, MarketRegime,
    MarketRegimeInputs, SpotInventoryRiskSnapshot,
};
use rustcta::scanner::{
    build_symbol_coverage, default_exchange_roles, scan_five_exchange_spot, ConfidenceLevel,
    ExchangeOperationalRole, FiveExchangeSpotScannerConfig, SymbolCandidateRecommendedAction,
};
use std::collections::{BTreeMap, HashMap};

#[test]
fn coverage_should_find_two_and_five_exchange_symbols_without_enabling_them() {
    let roles = default_exchange_roles();
    let rules = vec![
        rule("gateio", "BTCUSDT"),
        rule("bitget", "BTCUSDT"),
        rule("mexc", "BTCUSDT"),
        rule("coinex", "BTCUSDT"),
        rule("kucoin", "BTCUSDT"),
        rule("gateio", "ETHUSDT"),
        rule("bitget", "ETHUSDT"),
    ];

    let coverage = build_symbol_coverage(&rules, &roles);

    let btc = coverage
        .iter()
        .find(|item| item.internal_symbol == "BTCUSDT")
        .unwrap();
    assert_eq!(btc.available_exchanges.len(), 5);
    assert_eq!(
        btc.future_execution_candidate_exchanges,
        vec!["bitget", "gateio"]
    );
    let eth = coverage
        .iter()
        .find(|item| item.internal_symbol == "ETHUSDT")
        .unwrap();
    assert_eq!(eth.available_exchanges.len(), 2);
}

#[test]
fn scanner_should_evaluate_directed_pairs_and_keep_live_execution_disabled() {
    let config = FiveExchangeSpotScannerConfig::default();
    let rules = ["gateio", "bitget", "mexc", "coinex", "kucoin"]
        .into_iter()
        .map(|exchange| rule(exchange, "BTCUSDT"))
        .collect::<Vec<_>>();
    let books = vec![
        book("gateio", "BTCUSDT", 100.0, 101.0),
        book("bitget", "BTCUSDT", 103.0, 104.0),
        book("mexc", "BTCUSDT", 102.0, 103.0),
        book("coinex", "BTCUSDT", 101.0, 102.0),
        book("kucoin", "BTCUSDT", 100.5, 101.5),
    ];

    let read_model = scan_five_exchange_spot(&config, &rules, &books, &fee_model());

    assert!(read_model.opportunities.len() >= 20 * config.notionals_usdt.len());
    let gate_to_bitget = read_model
        .opportunities
        .iter()
        .find(|item| {
            item.buy_exchange == "gateio"
                && item.sell_exchange == "bitget"
                && (item.target_notional - 1.0).abs() < f64::EPSILON
        })
        .unwrap();
    assert!(gate_to_bitget.tt_net_bps > 0.0);
    assert!(gate_to_bitget.execution_eligibility.live_dry_run_eligible);
    assert!(
        !gate_to_bitget
            .execution_eligibility
            .currently_live_executable
    );
    let mexc_to_bitget = read_model
        .opportunities
        .iter()
        .find(|item| item.buy_exchange == "mexc" && item.sell_exchange == "bitget")
        .unwrap();
    assert!(!mexc_to_bitget.execution_eligibility.live_dry_run_eligible);
    assert_eq!(
        mexc_to_bitget.buy_exchange_role,
        ExchangeOperationalRole::ScanOnly
    );
}

#[test]
fn scanner_should_use_buy_asks_sell_bids_and_fee_model() {
    let config = FiveExchangeSpotScannerConfig {
        notionals_usdt: vec![10.0],
        ..FiveExchangeSpotScannerConfig::default()
    };
    let rules = vec![rule("gateio", "BTCUSDT"), rule("bitget", "BTCUSDT")];
    let books = vec![
        book_with_depth(
            "gateio",
            "BTCUSDT",
            vec![(100.0, 0.05), (110.0, 1.0)],
            vec![(90.0, 1.0)],
        ),
        book_with_depth("bitget", "BTCUSDT", vec![(120.0, 1.0)], vec![(115.0, 1.0)]),
    ];

    let read_model = scan_five_exchange_spot(&config, &rules, &books, &fee_model());
    let opportunity = read_model
        .opportunities
        .iter()
        .find(|item| item.buy_exchange == "gateio" && item.sell_exchange == "bitget")
        .unwrap();

    assert!(opportunity.buy_vwap > 100.0);
    assert_eq!(opportunity.sell_vwap, 115.0);
    assert_eq!(opportunity.total_taker_fee_bps, 20.0);
}

#[test]
fn low_samples_should_keep_symbol_score_low_confidence() {
    let config = FiveExchangeSpotScannerConfig::default();
    let rules = vec![rule("gateio", "BTCUSDT"), rule("bitget", "BTCUSDT")];
    let books = vec![
        book("gateio", "BTCUSDT", 100.0, 101.0),
        book("bitget", "BTCUSDT", 103.0, 104.0),
    ];

    let read_model = scan_five_exchange_spot(&config, &rules, &books, &fee_model());
    let score = read_model.symbol_scores.first().unwrap();

    assert_eq!(score.confidence, ConfidenceLevel::Low);
    assert_eq!(
        score.recommended_action,
        SymbolCandidateRecommendedAction::InsufficientData
    );
}

#[test]
fn hedge_policy_should_remain_recommendation_only() {
    let risk = SpotInventoryRiskSnapshot {
        symbol: "BTCUSDT".to_string(),
        total_managed_spot_quantity: 0.1,
        inventory_value_usdt: 10_000.0,
        inventory_cost_basis_optional: None,
        unrealized_pnl_optional: None,
        exchange_distribution: BTreeMap::from([("gateio".to_string(), 1.0)]),
        concentration_score: 80.0,
        volatility_estimate: 700.0,
        drawdown_estimate: 600.0,
        beta_to_btc_optional: Some(1.0),
        hedge_instrument_available: true,
        hedge_market_depth_optional: None,
        funding_rate_optional: Some(1.0),
        basis_bps_optional: Some(3.0),
        warnings: Vec::new(),
    };
    let regime = classify_market_regime(&MarketRegimeInputs {
        btc_above_fast_ma: Some(false),
        btc_above_slow_ma: Some(false),
        drawdown_bps: Some(700.0),
        realized_volatility_bps: Some(500.0),
        ..MarketRegimeInputs::default()
    });
    assert_eq!(regime.regime, MarketRegime::Downtrend);

    let recommendation = recommend_hedge(
        &risk,
        HedgePolicyMode::NoHedge,
        &regime,
        &[HedgeVenueCapability {
            exchange: "bitget".to_string(),
            supports_perpetual: true,
            supports_unified_account: false,
            supports_spot_as_collateral: false,
            supports_cross_margin: true,
            supports_isolated_margin: true,
            supports_portfolio_margin: false,
            supports_internal_transfer: true,
            collateral_haircuts_known: false,
            funding_data_available: true,
            mark_price_available: true,
            index_price_available: true,
            supported_hedge_symbols: vec!["BTCUSDT-PERP".to_string()],
            account_structure: "separate".to_string(),
            collateral_rules: Vec::new(),
            warnings: vec![
                "collateral haircut unknown; no unified account benefit assumed".to_string(),
            ],
        }],
    );

    assert!(recommendation.recommended_hedge_ratio > 0.0);
    assert!(recommendation
        .blockers
        .iter()
        .any(|item| item.contains("execution is disabled")));
}

#[tokio::test]
async fn kucoin_adapter_should_require_credentials_before_mutation() {
    let client = rustcta::exchanges::KuCoinSpotClient::new(Default::default());
    let result = rustcta::exchanges::unified::ExchangeClient::place_order(
        &client,
        OrderRequest::spot_market_buy("BTCUSDT", 0.001),
    )
    .await;
    assert!(matches!(
        result,
        Err(ExchangeClientError::Classified(ExchangeError {
            class: ExchangeErrorClass::AuthenticationFailed,
            ..
        }))
    ));
}

fn fee_model() -> FeeModel {
    let mut defaults = HashMap::new();
    for exchange in ["gateio", "bitget", "mexc", "coinex", "kucoin"] {
        defaults.insert(
            exchange.to_string(),
            HashMap::from([(
                "spot".to_string(),
                FeePairConfig {
                    maker_bps: 5.0,
                    taker_bps: 10.0,
                    fee_asset: Some("quote".to_string()),
                    rebate_ratio: None,
                },
            )]),
        );
    }
    FeeModel::from_config(FeeConfig {
        defaults,
        ..FeeConfig::default()
    })
}

fn rule(exchange: &str, symbol: &str) -> SymbolRule {
    SymbolRule {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        internal_symbol: symbol.to_string(),
        exchange_symbol: symbol.to_string(),
        base_asset: symbol.trim_end_matches("USDT").to_string(),
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

fn book(exchange: &str, symbol: &str, bid: f64, ask: f64) -> OrderBookSnapshot {
    book_with_depth(exchange, symbol, vec![(ask, 10.0)], vec![(bid, 10.0)])
}

fn book_with_depth(
    exchange: &str,
    symbol: &str,
    asks: Vec<(f64, f64)>,
    bids: Vec<(f64, f64)>,
) -> OrderBookSnapshot {
    let bids = bids
        .into_iter()
        .map(|(price, quantity)| OrderBookLevel { price, quantity })
        .collect::<Vec<_>>();
    let asks = asks
        .into_iter()
        .map(|(price, quantity)| OrderBookLevel { price, quantity })
        .collect::<Vec<_>>();
    OrderBookSnapshot {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_string(),
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp: Some(Utc::now()),
        received_at: Utc::now(),
        latency_ms: Some(1),
        sequence: Some(1),
        is_stale: false,
    }
}
