#![allow(clippy::all)]
use chrono::{Duration, Utc};
use rustcta_strategy_sdk::OrderSide;
use rustcta_strategy_trend::{
    calculate_stop_loss, calculate_take_profits, validate_signal_quality, SignalMetadata,
    SignalType, TakeProfit, TradeSignal,
};

fn signal(confidence: f64, risk_reward_ratio: f64) -> TradeSignal {
    TradeSignal {
        symbol: "BTCUSDT".to_string(),
        signal_type: SignalType::TrendBreakout,
        side: OrderSide::Buy,
        entry_price: 100.0,
        stop_loss: 98.0,
        take_profits: vec![TakeProfit {
            price: 104.0,
            ratio: 1.0,
        }],
        suggested_size: 0.1,
        risk_reward_ratio,
        confidence,
        timeframe_aligned: true,
        has_structure_support: true,
        expire_time: Utc::now() + Duration::minutes(5),
        metadata: SignalMetadata {
            trend_strength: 80.0,
            volume_confirmed: true,
            key_level_nearby: true,
            pattern_name: Some("breakout".to_string()),
            generated_at: Utc::now(),
        },
    }
}

#[test]
fn trend_core_should_calculate_directional_stops_and_targets() {
    let long_stop = calculate_stop_loss(100.0, OrderSide::Buy, 0.02);
    let short_stop = calculate_stop_loss(100.0, OrderSide::Sell, 0.02);

    assert_eq!(long_stop, 98.0);
    assert_eq!(short_stop, 102.0);

    let long_targets = calculate_take_profits(100.0, long_stop, OrderSide::Buy);
    let short_targets = calculate_take_profits(100.0, short_stop, OrderSide::Sell);

    assert!(long_targets.iter().all(|target| target.price > 100.0));
    assert!(short_targets.iter().all(|target| target.price < 100.0));
}

#[test]
fn trend_core_should_gate_low_quality_signals() {
    assert!(validate_signal_quality(&signal(75.0, 2.5)));
    assert!(!validate_signal_quality(&signal(50.0, 2.5)));
    assert!(!validate_signal_quality(&signal(75.0, 1.2)));
}
