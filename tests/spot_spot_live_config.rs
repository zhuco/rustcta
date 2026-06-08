#![allow(clippy::all)]
use std::fs;

use rustcta_strategy_spot_spot_arbitrage::SpotSpotTakerArbitrageConfig;

#[test]
fn live_small_spot_config_should_run_manual_vsn_only() {
    let path = format!(
        "{}/config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = fs::read_to_string(path).expect("read spot live config");
    let config: SpotSpotTakerArbitrageConfig =
        serde_yaml::from_str(&raw).expect("parse spot live config");

    assert_eq!(config.trading_mode, "live");
    assert_eq!(config.symbols, vec!["VSNUSDT"]);
    assert_eq!(config.websocket.symbols, config.symbols);
    assert_eq!(config.small_live_gate.enabled_symbols, config.symbols);
    assert_eq!(
        config.live_preflight.symbols,
        config.small_live_gate.enabled_symbols
    );
    assert_eq!(config.max_enabled_arbitrage_symbols, 1);
    assert_eq!(config.initial_entry_notional_usdt, 20.0);
    assert_eq!(config.active_taker_notional_usdt, 3.6);
    assert_eq!(config.max_notional_per_trade, 3.6);
    assert_eq!(config.live_dry_run.max_notional_per_order, 14.0);
    assert_eq!(config.live_dry_run.max_total_notional, 50.0);
    assert_eq!(config.small_live_gate.max_notional_per_order, 14.0);
    assert_eq!(config.small_live_gate.max_total_notional, 50.0);
    assert_eq!(config.live_preflight.max_live_notional_per_trade, Some(3.6));
    assert_eq!(config.live_preflight.max_total_live_notional, Some(50.0));
    assert_eq!(config.max_notional_per_symbol, 50.0);
    assert_eq!(config.max_total_notional, 50.0);
    assert_eq!(config.min_depth_notional, 3.6);
    assert_eq!(config.max_raw_spread_bps, 1_000.0);
    assert_eq!(config.inventory_rebalance.target_total_notional_usdt, 20.0);
    assert!(config.inventory_rebalance.enabled);
    assert!(config.inventory_rebalance.allow_market_rebalance);
    assert!(!config.inventory_rebalance.allow_auto_initial_entry);
    assert!(!config.inventory_rebalance.allow_auto_exit);
    assert!(!config.inventory_rebalance.allow_profit_covered_recovery);
    assert!(!config.inventory_rebalance.allow_emergency_recovery);
    config
        .validate_safe_mode()
        .expect("spot live config should pass safe-mode validation");
}
