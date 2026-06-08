#![allow(clippy::all)]
use std::fs;

use rustcta_strategy_spot_spot_arbitrage::SpotSpotTakerArbitrageConfig;

fn load_live_config() -> SpotSpotTakerArbitrageConfig {
    let path = format!(
        "{}/config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = fs::read_to_string(path).expect("read spot live config");
    serde_yaml::from_str(&raw).expect("parse spot live config")
}

#[test]
fn live_preflight_contract_should_accept_configured_live_gate() {
    let config = load_live_config();

    config
        .validate_safe_mode()
        .expect("configured live preflight gate should pass safe-mode validation");
    assert_eq!(config.trading_mode, "live");
    assert!(config.live_preflight.enabled);
    assert!(config.kill_switch.allow_live_orders);
    assert_eq!(
        config.live_preflight.symbols,
        config.small_live_gate.enabled_symbols
    );
}

#[test]
fn live_preflight_contract_should_block_live_when_disabled() {
    let mut config = load_live_config();
    config.live_preflight.enabled = false;

    let error = config
        .validate_safe_mode()
        .expect_err("live mode should require live_preflight.enabled=true")
        .to_string();
    assert!(error.contains("live_preflight.enabled=true"));
}
