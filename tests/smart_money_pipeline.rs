#![allow(clippy::all)]
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rustcta_smart_money::*;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

fn wallet_id() -> WalletId {
    WalletId(Uuid::new_v4())
}

fn profile(id: WalletId) -> WalletProfile {
    WalletProfile {
        wallet_id: id,
        as_of: Utc::now(),
        total_return: Decimal::new(40, 2),
        return_30d: Decimal::new(20, 2),
        return_90d: Decimal::new(35, 2),
        return_180d: Decimal::new(50, 2),
        sharpe: Some(Decimal::new(2, 0)),
        sortino: Some(Decimal::new(3, 0)),
        calmar: Some(Decimal::new(2, 0)),
        max_drawdown: Decimal::new(10, 2),
        win_rate: Decimal::new(62, 2),
        profit_factor: Decimal::new(180, 2),
        average_holding_secs: 8 * 60 * 60,
        average_leverage: Decimal::new(3, 0),
        maximum_leverage: Decimal::new(6, 0),
        trade_frequency_daily: Decimal::new(4, 0),
        position_concentration: Decimal::new(25, 2),
        risk_per_trade: Decimal::new(2, 2),
        signal_reproducibility: Decimal::new(75, 2),
        capital_efficiency: Decimal::new(70, 2),
        behavior_stability: Decimal::new(80, 2),
        recent_performance: Decimal::new(18, 2),
        closed_trades: 100,
        active_days: 50,
        history_days: 100,
        equity_usdt: Decimal::new(50_000, 0),
    }
}

fn regime(now: chrono::DateTime<Utc>) -> MarketRegimeSnapshot {
    MarketRegimeSnapshot {
        as_of: now,
        bull_trend: Decimal::new(70, 2),
        bear_trend: Decimal::ZERO,
        range: Decimal::new(10, 2),
        volatility_expansion: Decimal::new(25, 2),
        volatility_compression: Decimal::new(10, 2),
        risk_on: Decimal::new(60, 2),
        risk_off: Decimal::ZERO,
        funding_extreme: Decimal::ZERO,
        liquidity_crisis: Decimal::ZERO,
        trend_acceleration: Decimal::new(20, 2),
        trend_exhaustion: Decimal::ZERO,
    }
}

#[test]
fn hyperliquid_parser_should_extract_positions_and_fills() {
    let id = wallet_id();
    let now = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let state = json!({
        "marginSummary": {
            "accountValue": "50000.0",
            "totalMarginUsed": "2500.0"
        },
        "withdrawable": "45000.0",
        "assetPositions": [{
            "position": {
                "coin": "BTC",
                "szi": "0.5",
                "entryPx": "40000.0",
                "positionValue": "21000.0",
                "unrealizedPnl": "1000.0",
                "marginUsed": "2100.0",
                "leverage": {"type": "cross", "value": 4}
            }
        }]
    });

    let parsed = parse_clearinghouse_state(id.clone(), "0xabc", &state, now).expect("parsed state");

    assert_eq!(parsed.equity_usdt, Decimal::new(500000, 1));
    assert_eq!(parsed.positions.len(), 1);
    assert_eq!(parsed.positions[0].symbol, "BTCUSDT");
    assert_eq!(parsed.positions[0].direction, Direction::Long);
    assert_eq!(parsed.positions[0].leverage, Decimal::new(4, 0));

    let fills = json!([{
        "coin": "BTC",
        "px": "40100.0",
        "sz": "0.1",
        "side": "B",
        "time": 1700000000000i64,
        "fee": "1.604",
        "closedPnl": "0.0",
        "hash": "0xfill"
    }]);
    let trades = parse_user_fills(id, &fills).expect("parsed fills");

    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].symbol, "BTCUSDT");
    assert_eq!(trades[0].notional_usdt, Decimal::new(401000, 2));
}

#[test]
fn binance_market_parser_should_parse_combined_trade_and_mark_streams() {
    let trade = parse_binance_market_stream(
        r#"{
            "stream":"btcusdt@trade",
            "data":{
                "e":"trade","E":1700000000000,"s":"BTCUSDT","t":100,
                "p":"40000.50","q":"0.250","m":false
            }
        }"#,
    )
    .expect("trade event");
    match trade {
        BinanceMarketStreamEvent::Trade(tick) => {
            assert_eq!(tick.symbol, "BTCUSDT");
            assert_eq!(tick.taker_side(), "buy");
            assert_eq!(tick.notional_usdt(), Decimal::new(10000125, 3));
        }
        _ => panic!("expected trade"),
    }

    let path = build_combined_stream_path(&["BTCUSDT".to_string(), "ETHUSDT".to_string()], 100);
    assert!(path.contains("btcusdt@depth@100ms"));
    assert!(path.contains("ethusdt@markPrice"));
}

#[test]
fn smart_money_pipeline_should_generate_risk_approved_target() {
    let now = Utc::now();
    let id_a = wallet_id();
    let id_b = wallet_id();
    let profiles = vec![profile(id_a.clone()), profile(id_b.clone())];
    let positions = vec![
        WalletPositionSnapshot {
            wallet_id: id_a,
            symbol: "BTCUSDT".to_string(),
            direction: Direction::Long,
            quantity: Decimal::new(1, 0),
            entry_price: Decimal::new(40000, 0),
            mark_price: Decimal::new(41000, 0),
            notional_usdt: Decimal::new(2000, 0),
            equity_usdt: Decimal::new(50000, 0),
            margin_used_usdt: Decimal::new(200, 0),
            leverage: Decimal::new(4, 0),
            unrealized_pnl_usdt: Decimal::new(1000, 0),
            observed_at: now,
        },
        WalletPositionSnapshot {
            wallet_id: id_b,
            symbol: "BTCUSDT".to_string(),
            direction: Direction::Long,
            quantity: Decimal::new(1, 0),
            entry_price: Decimal::new(40000, 0),
            mark_price: Decimal::new(41000, 0),
            notional_usdt: Decimal::new(1800, 0),
            equity_usdt: Decimal::new(50000, 0),
            margin_used_usdt: Decimal::new(180, 0),
            leverage: Decimal::new(4, 0),
            unrealized_pnl_usdt: Decimal::new(900, 0),
            observed_at: now,
        },
    ];

    let output = run_smart_money_pipeline(
        &profiles,
        &positions,
        &regime(now),
        now,
        Decimal::new(2000, 0),
        &PortfolioConstraints::default(),
        &SmartMoneyPipelineConfig::default(),
        Decimal::ZERO,
        Decimal::ZERO,
        false,
    );

    assert_eq!(output.wallet_research.len(), 2);
    assert_eq!(output.opinions.len(), 2);
    assert_eq!(output.alpha.len(), 1);
    assert_eq!(output.risk_decision.kind, RiskDecisionKind::Approved);
    assert!(output.target_portfolio.gross_notional_usdt > Decimal::ZERO);
    assert!(output.target_portfolio.leverage <= Decimal::new(10, 0));
}

#[test]
fn rebalance_simulation_should_execute_target_against_books() {
    let now = Utc::now();
    let mut target_notional = BTreeMap::new();
    target_notional.insert("BTCUSDT".to_string(), Decimal::new(1000, 0));
    let target = TargetPortfolio {
        portfolio_id: Uuid::new_v4(),
        as_of: now,
        nav_usdt: Decimal::new(2000, 0),
        gross_notional_usdt: Decimal::new(1000, 0),
        leverage: Decimal::new(5, 1),
        target_weights: BTreeMap::new(),
        target_notional,
        cash_weight: Decimal::new(5, 1),
    };
    let mut books = HashMap::new();
    books.insert(
        "BTCUSDT".to_string(),
        OrderBookSnapshot {
            symbol: "BTCUSDT".to_string(),
            ts: now,
            bids: vec![BookLevel {
                price: Decimal::new(39990, 0),
                quantity: Decimal::new(1, 0),
            }],
            asks: vec![BookLevel {
                price: Decimal::new(40000, 0),
                quantity: Decimal::new(1, 0),
            }],
        },
    );

    let mut ledger = PortfolioLedger::new(Decimal::new(2000, 0), Decimal::new(10, 0));
    let report = simulate_rebalance_to_target(
        &mut ledger,
        &BTreeMap::new(),
        &target,
        &books,
        &TakerExecutionConfig::default(),
    )
    .expect("simulation");

    assert_eq!(report.fills.len(), 1);
    assert_eq!(report.missing_books.len(), 0);
    assert!(report.final_nav_usdt < Decimal::new(2000, 0));
    assert!(report.final_gross_notional_usdt > Decimal::ZERO);
}
