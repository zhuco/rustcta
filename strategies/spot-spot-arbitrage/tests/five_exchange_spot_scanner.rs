#![allow(clippy::all)]
use rustcta_strategy_spot_spot_arbitrage::{
    calculate_spread, configured_spot_pair, depth_notional, SpotOrderBookLevel, SpotVenue,
};

#[test]
fn scanner_boundary_should_keep_gateio_bitget_as_execution_pair() {
    let exchanges = vec![
        "gateio".to_string(),
        "bitget".to_string(),
        "mexc".to_string(),
        "coinex".to_string(),
        "kucoin".to_string(),
    ];

    let (left, right) = configured_spot_pair(&exchanges);

    assert_eq!(left, SpotVenue::GateIo);
    assert_eq!(right, SpotVenue::Bitget);
}

#[test]
fn scanner_boundary_should_use_buy_asks_sell_bids_and_depth_notional() {
    let spread = calculate_spread(100.0, 103.0, 10.0, 10.0, 0.0, 0.0);
    assert!(spread.raw_spread_bps > 0.0);
    assert!(spread.net_spread_bps > 0.0);

    let asks = vec![
        SpotOrderBookLevel {
            price: 100.0,
            quantity: 0.05,
        },
        SpotOrderBookLevel {
            price: 101.0,
            quantity: 0.10,
        },
    ];

    let executable = depth_notional(&asks, 10.0);

    assert!(executable >= 10.0);
    assert!(executable <= 100.0 * 0.05 + 101.0 * 0.10);
}
