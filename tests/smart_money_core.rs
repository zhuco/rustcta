#![allow(clippy::all)]
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rustcta::smart_money::*;
use std::collections::BTreeMap;
use uuid::Uuid;

fn wallet_id() -> WalletId {
    WalletId(Uuid::new_v4())
}

fn baseline_profile() -> WalletProfile {
    WalletProfile {
        wallet_id: wallet_id(),
        as_of: Utc::now(),
        total_return: Decimal::new(40, 2),
        return_30d: Decimal::new(12, 2),
        return_90d: Decimal::new(30, 2),
        return_180d: Decimal::new(60, 2),
        sharpe: Some(Decimal::new(2, 0)),
        sortino: Some(Decimal::new(3, 0)),
        calmar: Some(Decimal::new(2, 0)),
        max_drawdown: Decimal::new(12, 2),
        win_rate: Decimal::new(58, 2),
        profit_factor: Decimal::new(160, 2),
        average_holding_secs: 3600,
        average_leverage: Decimal::new(3, 0),
        maximum_leverage: Decimal::new(7, 0),
        trade_frequency_daily: Decimal::new(2, 0),
        position_concentration: Decimal::new(30, 2),
        risk_per_trade: Decimal::new(2, 2),
        signal_reproducibility: Decimal::new(70, 2),
        capital_efficiency: Decimal::new(65, 2),
        behavior_stability: Decimal::new(72, 2),
        recent_performance: Decimal::new(15, 2),
        closed_trades: 120,
        active_days: 60,
        history_days: 120,
        equity_usdt: Decimal::new(50000, 0),
    }
}

fn baseline_regime() -> MarketRegimeSnapshot {
    MarketRegimeSnapshot {
        as_of: Utc::now(),
        bull_trend: Decimal::new(70, 2),
        bear_trend: Decimal::new(5, 2),
        range: Decimal::new(10, 2),
        volatility_expansion: Decimal::new(40, 2),
        volatility_compression: Decimal::new(10, 2),
        risk_on: Decimal::new(65, 2),
        risk_off: Decimal::new(5, 2),
        funding_extreme: Decimal::ZERO,
        liquidity_crisis: Decimal::ZERO,
        trend_acceleration: Decimal::new(30, 2),
        trend_exhaustion: Decimal::new(5, 2),
    }
}

#[test]
fn smart_money_filter_should_reject_low_quality_wallets() {
    let mut profile = baseline_profile();
    profile.equity_usdt = Decimal::new(1000, 0);
    profile.closed_trades = 3;

    let decision = filter_wallet_profile(&profile, &WalletFilterConfig::default());

    assert!(!decision.accepted);
    assert!(decision
        .reasons
        .contains(&FilterRejectReason::WalletBelowEquity));
    assert!(decision
        .reasons
        .contains(&FilterRejectReason::InsufficientClosedTrades));
}

#[test]
fn smart_money_scoring_should_generate_bounded_score_matrix() {
    let profile = baseline_profile();
    let score = score_wallet(&profile, &baseline_regime(), &ScoreWeights::default());

    assert!(score.final_score >= Decimal::ZERO);
    assert!(score.final_score <= Decimal::ONE);
    assert!(score.current_regime_score > Decimal::ZERO);
}

#[test]
fn smart_money_alpha_should_aggregate_wallet_opinions() {
    let now = Utc::now();
    let opinions = vec![
        WalletOpinion {
            wallet_id: wallet_id(),
            symbol: "BTCUSDT".to_string(),
            direction: Direction::Long,
            confidence: Decimal::new(80, 2),
            conviction: Decimal::new(70, 2),
            dynamic_score: Decimal::new(90, 2),
            wallet_equity_usdt: Decimal::new(100000, 0),
            clusters: vec![TraderCluster::Trend],
            observed_at: now,
            expires_at: now + Duration::minutes(5),
        },
        WalletOpinion {
            wallet_id: wallet_id(),
            symbol: "BTCUSDT".to_string(),
            direction: Direction::Short,
            confidence: Decimal::new(20, 2),
            conviction: Decimal::new(30, 2),
            dynamic_score: Decimal::new(50, 2),
            wallet_equity_usdt: Decimal::new(50000, 0),
            clusters: vec![TraderCluster::Scalper],
            observed_at: now,
            expires_at: now + Duration::minutes(5),
        },
    ];

    let alpha = aggregate_alpha(&opinions, now, &AlphaAggregationConfig::default());

    assert_eq!(alpha.len(), 1);
    assert_eq!(alpha[0].symbol, "BTCUSDT");
    assert!(alpha[0].long_pressure > alpha[0].short_pressure);
    assert!(alpha[0].net_alpha_score > Decimal::ZERO);
}

#[test]
fn smart_money_portfolio_should_enforce_capital_constraints() {
    let now = Utc::now();
    let alpha = vec![
        AggregatedAlpha {
            symbol: "BTCUSDT".to_string(),
            as_of: now,
            long_pressure: Decimal::ONE,
            short_pressure: Decimal::ZERO,
            consensus_strength: Decimal::ONE,
            capital_weighted_consensus: Decimal::ONE,
            cluster_weighted_consensus: Decimal::ONE,
            net_alpha_score: Decimal::ONE,
            alpha_confidence_score: Decimal::ONE,
            contributing_wallets: 10,
            dominant_wallet_share: Decimal::new(10, 2),
            dominant_cluster_share: Decimal::new(20, 2),
        },
        AggregatedAlpha {
            symbol: "ETHUSDT".to_string(),
            as_of: now,
            long_pressure: Decimal::ZERO,
            short_pressure: Decimal::ONE,
            consensus_strength: Decimal::ONE,
            capital_weighted_consensus: -Decimal::ONE,
            cluster_weighted_consensus: Decimal::ONE,
            net_alpha_score: -Decimal::ONE,
            alpha_confidence_score: Decimal::ONE,
            contributing_wallets: 10,
            dominant_wallet_share: Decimal::new(10, 2),
            dominant_cluster_share: Decimal::new(20, 2),
        },
    ];
    let constraints = PortfolioConstraints::default();

    let portfolio = construct_target_portfolio(
        &alpha,
        Decimal::new(2000, 0),
        now,
        &constraints,
        &PortfolioConstructionConfig::default(),
    );

    assert!(portfolio.gross_notional_usdt <= Decimal::new(20000, 0));
    assert!(portfolio.leverage <= Decimal::new(10, 0));
    assert_eq!(portfolio.target_notional.len(), 2);
}

#[test]
fn smart_money_execution_sim_should_sweep_book_and_charge_fee() {
    let book = OrderBookSnapshot {
        symbol: "BTCUSDT".to_string(),
        ts: Utc::now(),
        bids: vec![BookLevel {
            price: Decimal::new(9990, 0),
            quantity: Decimal::new(1, 0),
        }],
        asks: vec![
            BookLevel {
                price: Decimal::new(10000, 0),
                quantity: Decimal::new(5, 2),
            },
            BookLevel {
                price: Decimal::new(10010, 0),
                quantity: Decimal::new(10, 2),
            },
        ],
    };

    let fill = simulate_taker_market_order(
        &book,
        Direction::Long,
        Decimal::new(1000, 0),
        &TakerExecutionConfig::default(),
    )
    .unwrap();

    assert_eq!(fill.filled_notional_usdt, Decimal::new(1000, 0));
    assert_eq!(fill.fee_usdt, Decimal::new(4, 1));
    assert_eq!(fill.levels_consumed, 2);
    assert!(!fill.partial_fill);
}

#[test]
fn smart_money_transitions_should_classify_reverse() {
    let mut current = BTreeMap::new();
    current.insert("SOLUSDT".to_string(), Decimal::new(1000, 0));
    let mut target = BTreeMap::new();
    target.insert("SOLUSDT".to_string(), Decimal::new(-500, 0));

    let transitions = plan_portfolio_transitions(&current, &target);

    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0].action, PortfolioAction::Reverse);
}

#[test]
fn smart_money_profile_should_compute_core_metrics_from_history() {
    let now = Utc::now();
    let id = wallet_id();
    let equity_curve = vec![
        EquityPoint {
            ts: now - Duration::days(90),
            equity_usdt: Decimal::new(10000, 0),
        },
        EquityPoint {
            ts: now - Duration::days(60),
            equity_usdt: Decimal::new(11000, 0),
        },
        EquityPoint {
            ts: now - Duration::days(30),
            equity_usdt: Decimal::new(10500, 0),
        },
        EquityPoint {
            ts: now,
            equity_usdt: Decimal::new(13000, 0),
        },
    ];
    let closed_positions = vec![
        ClosedPositionSample {
            symbol: "BTCUSDT".to_string(),
            direction: Direction::Long,
            opened_at: now - Duration::days(10),
            closed_at: now - Duration::days(9),
            entry_notional_usdt: Decimal::new(1000, 0),
            realized_pnl_usdt: Decimal::new(100, 0),
            max_leverage: Decimal::new(3, 0),
        },
        ClosedPositionSample {
            symbol: "ETHUSDT".to_string(),
            direction: Direction::Short,
            opened_at: now - Duration::days(8),
            closed_at: now - Duration::days(7),
            entry_notional_usdt: Decimal::new(1000, 0),
            realized_pnl_usdt: Decimal::new(-50, 0),
            max_leverage: Decimal::new(4, 0),
        },
    ];
    let trades = closed_positions
        .iter()
        .enumerate()
        .map(|(idx, position)| WalletTrade {
            wallet_id: id.clone(),
            symbol: position.symbol.clone(),
            direction: position.direction,
            price: Decimal::new(100, 0),
            quantity: Decimal::new(1, 0),
            notional_usdt: position.entry_notional_usdt,
            fee_usdt: Decimal::new(4, 1),
            realized_pnl_usdt: position.realized_pnl_usdt,
            executed_at: now - Duration::days(idx as i64),
            external_id: None,
        })
        .collect::<Vec<_>>();

    let profile = compute_wallet_profile(
        &WalletProfileInput {
            wallet_id: id,
            as_of: now,
            equity_curve,
            trades,
            closed_positions,
        },
        &ProfileComputationConfig::default(),
    );

    assert_eq!(profile.closed_trades, 2);
    assert!(profile.total_return > Decimal::ZERO);
    assert!(profile.max_drawdown > Decimal::ZERO);
    assert_eq!(profile.equity_usdt, Decimal::new(13000, 0));
}

#[test]
fn smart_money_clustering_should_assign_multiple_labels() {
    let mut profile = baseline_profile();
    profile.average_holding_secs = 20 * 60;
    profile.trade_frequency_daily = Decimal::new(25, 0);
    profile.win_rate = Decimal::new(65, 2);
    profile.max_drawdown = Decimal::new(10, 2);

    let clusters = classify_wallet(&profile, &ClusterConfig::default());
    let labels = clusters
        .iter()
        .map(|assignment| assignment.cluster.clone())
        .collect::<Vec<_>>();

    assert!(labels.contains(&TraderCluster::Scalper));
    assert!(labels.contains(&TraderCluster::ConsistentAlpha));
}

#[test]
fn smart_money_ledger_should_apply_open_and_close_fills() {
    let now = Utc::now();
    let mut ledger = PortfolioLedger::new(Decimal::new(2000, 0), Decimal::new(10, 0));
    let open = SimulatedFill {
        symbol: "BTCUSDT".to_string(),
        direction: Direction::Long,
        requested_notional_usdt: Decimal::new(1000, 0),
        filled_notional_usdt: Decimal::new(1000, 0),
        filled_quantity: Decimal::new(1, 0),
        average_price: Some(Decimal::new(1000, 0)),
        fee_usdt: Decimal::new(4, 1),
        slippage_bps: Decimal::ZERO,
        levels_consumed: 1,
        partial_fill: false,
        filled_at: now,
    };
    ledger.apply_fill(&open);
    let close = SimulatedFill {
        direction: Direction::Short,
        average_price: Some(Decimal::new(1100, 0)),
        fee_usdt: Decimal::new(44, 2),
        filled_at: now,
        ..open.clone()
    };
    ledger.apply_fill(&close);

    assert!(ledger.positions.is_empty());
    assert_eq!(ledger.nav.realized_pnl_usdt, Decimal::new(100, 0));
    assert_eq!(ledger.nav.fees_paid_usdt, Decimal::new(84, 2));
}
