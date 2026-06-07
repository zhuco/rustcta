use chrono::Utc;

use crate::exchanges::unified::{
    MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide, OrderType, SymbolRule, SymbolStatus,
    TimeInForce,
};
use crate::execution::{FeeConfig, FeeModel, FeePairConfig, FeeRate, FeeSource};

use super::*;

fn book(exchange: &str, market_type: MarketType, bid: f64, ask: f64) -> OrderBookSnapshot {
    OrderBookSnapshot {
        exchange: exchange.to_string(),
        market_type,
        symbol: "BTCUSDT".to_string(),
        bids: vec![
            OrderBookLevel {
                price: bid,
                quantity: 1.0,
            },
            OrderBookLevel {
                price: bid - 1.0,
                quantity: 2.0,
            },
        ],
        asks: vec![
            OrderBookLevel {
                price: ask,
                quantity: 1.0,
            },
            OrderBookLevel {
                price: ask + 1.0,
                quantity: 2.0,
            },
        ],
        best_bid: Some(bid),
        best_ask: Some(ask),
        exchange_timestamp: Some(Utc::now()),
        received_at: Utc::now(),
        latency_ms: Some(5),
        sequence: Some(1),
        is_stale: false,
    }
}

fn rule(exchange: &str, market_type: MarketType) -> SymbolRule {
    SymbolRule {
        exchange: exchange.to_string(),
        market_type,
        internal_symbol: "BTCUSDT".to_string(),
        exchange_symbol: "BTCUSDT".to_string(),
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        price_precision: 2,
        quantity_precision: 4,
        tick_size: 0.01,
        step_size: 0.0001,
        min_quantity: 0.0001,
        min_notional: 5.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Limit, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

fn fee(exchange: &str, market_type: MarketType, maker: f64, taker: f64) -> FeeRate {
    FeeModel::from_config(FeeConfig {
        fallback: Default::default(),
        defaults: std::collections::HashMap::from([(
            exchange.to_string(),
            std::collections::HashMap::from([(
                match market_type {
                    MarketType::Spot => "spot".to_string(),
                    MarketType::Perpetual => "perpetual".to_string(),
                },
                FeePairConfig {
                    maker_bps: maker,
                    taker_bps: taker,
                    fee_asset: Some("quote".to_string()),
                    rebate_ratio: None,
                },
            )]),
        )]),
        symbol_overrides: Vec::new(),
        side_overrides: Vec::new(),
        vip_overrides: Vec::new(),
        exchange_api: Vec::new(),
        platform_tokens: Vec::new(),
        prefer_exchange_api_fees: false,
    })
    .summary_rates()
    .into_iter()
    .find(|rate| rate.exchange == exchange)
    .unwrap()
}

fn relationship(kind: ArbitrageRelationshipType) -> ArbitrageRelationship {
    let (buy_market, sell_market) = match kind {
        ArbitrageRelationshipType::SpotSpot => (MarketType::Spot, MarketType::Spot),
        ArbitrageRelationshipType::SpotPerp => (MarketType::Spot, MarketType::Perpetual),
        ArbitrageRelationshipType::PerpPerp => (MarketType::Perpetual, MarketType::Perpetual),
    };
    ArbitrageRelationship {
        relationship_type: kind,
        buy_leg: MarketLeg {
            exchange: "buyex".to_string(),
            market_type: buy_market,
            internal_symbol: "BTCUSDT".to_string(),
            exchange_symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            order_book: book("buyex", buy_market, 99.0, 100.0),
            fee_rate: fee("buyex", buy_market, 1.0, 10.0),
            symbol_rule: rule("buyex", buy_market),
            funding_rate_optional: (buy_market == MarketType::Perpetual).then_some(
                FundingRateInfo {
                    funding_rate_bps: -1.0,
                    next_funding_time: None,
                    interval_seconds: Some(28_800),
                    source: "fixture".to_string(),
                },
            ),
            margin_info_optional: None,
        },
        sell_leg: MarketLeg {
            exchange: "sellex".to_string(),
            market_type: sell_market,
            internal_symbol: "BTCUSDT".to_string(),
            exchange_symbol: "BTCUSDT".to_string(),
            side: OrderSide::Sell,
            order_book: book("sellex", sell_market, 102.0, 103.0),
            fee_rate: fee("sellex", sell_market, 1.0, 10.0),
            symbol_rule: rule("sellex", sell_market),
            funding_rate_optional: (sell_market == MarketType::Perpetual).then_some(
                FundingRateInfo {
                    funding_rate_bps: 2.0,
                    next_funding_time: None,
                    interval_seconds: Some(28_800),
                    source: "fixture".to_string(),
                },
            ),
            margin_info_optional: None,
        },
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        settlement_asset_optional: Some("USDT".to_string()),
    }
}

#[test]
fn executable_buy_consumes_asks_and_calculates_vwap() {
    let result = executable_vwap(
        &book("x", MarketType::Spot, 99.0, 100.0),
        &rule("x", MarketType::Spot),
        ExecutablePriceRequest {
            side: OrderSide::Buy,
            target_quantity: Some(2.0),
            target_notional: None,
            stale_book_ms: 1_000,
            quantity_rounding_tolerance: 1e-8,
        },
    );
    assert_eq!(result.levels_consumed, 2);
    assert_eq!(result.worst_price, Some(101.0));
    assert!((result.vwap.unwrap() - 100.5).abs() < 1e-9);
    assert!(result.slippage_bps > 0.0);
}

#[test]
fn executable_sell_consumes_bids_and_rejects_insufficient_depth() {
    let ok = executable_vwap(
        &book("x", MarketType::Spot, 99.0, 100.0),
        &rule("x", MarketType::Spot),
        ExecutablePriceRequest {
            side: OrderSide::Sell,
            target_quantity: Some(2.0),
            target_notional: None,
            stale_book_ms: 1_000,
            quantity_rounding_tolerance: 1e-8,
        },
    );
    assert_eq!(ok.levels_consumed, 2);
    assert!((ok.vwap.unwrap() - 98.5).abs() < 1e-9);
    let rejected = executable_vwap(
        &book("x", MarketType::Spot, 99.0, 100.0),
        &rule("x", MarketType::Spot),
        ExecutablePriceRequest {
            side: OrderSide::Sell,
            target_quantity: Some(10.0),
            target_notional: None,
            stale_book_ms: 1_000,
            quantity_rounding_tolerance: 1e-8,
        },
    );
    assert!(!rejected.full_fill_possible);
}

#[test]
fn same_quantity_is_enforced_on_both_legs() {
    let buy = consume_levels(
        OrderSide::Buy,
        &[OrderBookLevel {
            price: 10.0,
            quantity: 1.0,
        }],
        1.0,
        10.0,
    );
    let sell = consume_levels(
        OrderSide::Sell,
        &[OrderBookLevel {
            price: 11.0,
            quantity: 0.5,
        }],
        1.0,
        11.0,
    );
    assert!(enforce_same_quantity(&buy, &sell, 1e-8).is_err());
}

#[test]
fn taker_taker_spot_spot_net_deducts_fees_and_slippage() {
    let buy = consume_levels(
        OrderSide::Buy,
        &[OrderBookLevel {
            price: 100.0,
            quantity: 1.0,
        }],
        1.0,
        100.0,
    );
    let sell = consume_levels(
        OrderSide::Sell,
        &[OrderBookLevel {
            price: 101.0,
            quantity: 1.0,
        }],
        1.0,
        101.0,
    );
    let entry = analyze_taker_taker_entry(&buy, &sell, 10.0, 10.0, 1.0, 1.0);
    assert!((entry.raw_executable_spread_bps - 100.0).abs() < 1e-9);
    assert!((entry.immediate_net_bps - 78.0).abs() < 1e-9);
}

#[test]
fn spot_perp_and_perp_perp_lifecycle_use_basis_and_funding() {
    let cfg = ArbitrageScannerConfig::default();
    let spot_perp = analyze_relationship(
        &relationship(ArbitrageRelationshipType::SpotPerp),
        50.0,
        &cfg,
        None,
    );
    assert!(spot_perp.expected_funding_net_bps > 0.0);
    assert_eq!(
        spot_perp.relationship_type,
        ArbitrageRelationshipType::SpotPerp
    );
    let perp_perp = analyze_relationship(
        &relationship(ArbitrageRelationshipType::PerpPerp),
        50.0,
        &cfg,
        None,
    );
    assert_eq!(
        perp_perp.relationship_type,
        ArbitrageRelationshipType::PerpPerp
    );
    assert!(perp_perp.expected_exit_fee_bps > 0.0);
}

#[test]
fn maker_taker_theoretical_is_not_executable_and_ev_is_reduced_by_risk() {
    let hedge = consume_levels(
        OrderSide::Sell,
        &[OrderBookLevel {
            price: 101.0,
            quantity: 1.0,
        }],
        1.0,
        101.0,
    );
    let ev = MakerTakerExpectedValueModel {
        fill_probability: 0.1,
        expected_spread_at_fill_bps: 100.0,
        expected_adverse_selection_bps: 10.0,
        expected_residual_loss_bps: 5.0,
        ..MakerTakerExpectedValueModel::default()
    };
    let analysis = maker_buy_taker_sell(100.0, &hedge, 1.0, 10.0, &ev);
    assert!(analysis.non_executable_theoretical);
    assert!(analysis.expected_net_bps < analysis.theoretical_net_bps);
    let worse = MakerTakerExpectedValueModel {
        expected_adverse_selection_bps: 50.0,
        ..ev
    };
    assert!(
        maker_buy_taker_sell(100.0, &hedge, 1.0, 10.0, &worse).expected_net_bps
            < analysis.expected_net_bps
    );
}

#[test]
fn taker_buy_maker_sell_theoretical_uses_maker_sell_price() {
    let buy = consume_levels(
        OrderSide::Buy,
        &[OrderBookLevel {
            price: 100.0,
            quantity: 1.0,
        }],
        1.0,
        100.0,
    );
    let result = taker_buy_maker_sell(
        &buy,
        101.0,
        1.0,
        10.0,
        &MakerTakerExpectedValueModel::default(),
    );
    assert!(result.theoretical_spread_bps > 0.0);
    assert!(result.non_executable_theoretical);
}

#[test]
fn capital_model_handles_separate_unified_haircut_and_roc() {
    let separate = calculate_capital_requirement(&CapitalModelInput {
        relationship_type: ArbitrageRelationshipType::SpotPerp,
        buy_market_type: MarketType::Spot,
        sell_market_type: MarketType::Perpetual,
        target_notional_usdt: 100.0,
        account_structure: AccountStructure::SeparateAccounts,
        collateral_assets: Vec::new(),
        buy_exchange_capabilities: ExchangeCapitalCapabilities::default(),
        sell_exchange_capabilities: ExchangeCapitalCapabilities::default(),
        initial_margin_rate: 0.1,
        maintenance_margin_rate: 0.01,
        liquidation_buffer_bps: 100.0,
        fragmented_capital_penalty_bps: 10.0,
    });
    assert!(separate.effective_total_capital_required_usdt > 100.0);
    let unified = calculate_capital_requirement(&CapitalModelInput {
        account_structure: AccountStructure::UnifiedAccount,
        collateral_assets: vec![CollateralAsset {
            asset: "USDT".to_string(),
            total_balance: 100.0,
            available_balance: 100.0,
            collateral_enabled: true,
            collateral_haircut: 0.5,
            effective_collateral_value_usdt: 50.0,
        }],
        buy_exchange_capabilities: ExchangeCapitalCapabilities {
            supports_unified_account: true,
            supports_spot_as_collateral: true,
            ..ExchangeCapitalCapabilities::default()
        },
        sell_exchange_capabilities: ExchangeCapitalCapabilities {
            supports_unified_account: true,
            ..ExchangeCapitalCapabilities::default()
        },
        ..CapitalModelInput {
            relationship_type: ArbitrageRelationshipType::SpotPerp,
            buy_market_type: MarketType::Spot,
            sell_market_type: MarketType::Perpetual,
            target_notional_usdt: 100.0,
            account_structure: AccountStructure::SeparateAccounts,
            collateral_assets: Vec::new(),
            buy_exchange_capabilities: ExchangeCapitalCapabilities::default(),
            sell_exchange_capabilities: ExchangeCapitalCapabilities::default(),
            initial_margin_rate: 0.1,
            maintenance_margin_rate: 0.01,
            liquidation_buffer_bps: 100.0,
            fragmented_capital_penalty_bps: 10.0,
        }
    });
    assert!(unified.unified_account_benefit_usdt > 0.0);
    assert!(
        unified.effective_total_capital_required_usdt
            < separate.effective_total_capital_required_usdt
    );
    let roc = expected_return_on_capital(1.0, &separate);
    assert!(expected_return_on_capital_per_hour(roc, 3_600) > 0.0);
}

#[test]
fn score_components_are_explainable_and_penalize_fallback_stale() {
    let score = score_opportunity(&RiskScoreInput {
        expected_return_on_capital_per_hour: 0.01,
        full_depth_available: true,
        max_book_age_ms: 2_000,
        fee_fallback_used: true,
        convergence_confidence: ConfidenceLevel::Low,
        funding_confidence: ConfidenceLevel::Low,
        residual_risk_bps: 5.0,
        adverse_selection_bps: 10.0,
        capital_fragmentation_penalty_usdt: 20.0,
        stale_book: true,
    });
    assert!(score.stale_book_penalty > 0.0);
    assert!(score.fee_fallback_penalty > 0.0);
}

#[test]
fn opportunity_filter_keeps_distinct_raw_and_net_fields() {
    let analysis = analyze_relationship(
        &relationship(ArbitrageRelationshipType::SpotSpot),
        50.0,
        &ArbitrageScannerConfig::default(),
        Some(&ArbitrageStatisticsSnapshot {
            confidence: ConfidenceLevel::Medium,
            ..ArbitrageStatisticsSnapshot::default()
        }),
    );
    assert_ne!(
        analysis.raw_executable_spread_bps,
        analysis.tt_immediate_net_bps
    );
    assert!(!analysis.fee_sources.is_empty());
    assert!(!analysis.score_components.total_score.is_nan());
    let filtered = filter_opportunities(
        vec![analysis.clone()],
        ArbitrageOpportunityQuery {
            relationship_type: Some(ArbitrageRelationshipType::SpotSpot),
            symbol: Some("BTC_USDT".to_string()),
            exchange: Some("buyex".to_string()),
            min_tt_net_bps: Some(-10_000.0),
            min_lifecycle_expected_net_bps: None,
            min_return_on_capital_per_hour: None,
            confidence: Some(ConfidenceLevel::Low),
            accepted: None,
            fresh_only: Some(true),
        },
    );
    assert_eq!(filtered.len(), 1);
}

#[test]
fn missing_funding_and_fallback_fees_lower_confidence() {
    let mut rel = relationship(ArbitrageRelationshipType::SpotPerp);
    rel.sell_leg.funding_rate_optional = None;
    rel.buy_leg.fee_rate.source = FeeSource::Fallback;
    let analysis = analyze_relationship(&rel, 50.0, &ArbitrageScannerConfig::default(), None);
    assert_eq!(analysis.confidence, ConfidenceLevel::Low);
    assert!(analysis
        .warnings
        .iter()
        .any(|warning| warning.contains("funding")));
    assert!(analysis
        .warnings
        .iter()
        .any(|warning| warning.contains("fallback")));
}
