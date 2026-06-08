#![allow(clippy::all)]
use std::fs;
use std::process::Command;

use tempfile::tempdir;

fn base_config(live_preflight_extra: &str) -> String {
    format!(
        r#"
enabled: true
trading_mode: paper
exchanges: [mexc, coinex]
symbols: [BTCUSDT]
quote_asset: USDT
max_notional_per_trade: 25.0
min_notional_per_trade: 5.0
max_notional_per_symbol: 250.0
max_total_notional: 500.0
min_net_spread_bps: 5.0
taker_fee_bps_override: 10.0
min_depth_notional: 5.0
dry_run: true
live_trading_enabled: false
market_data_mode: rest_polling
websocket:
  enabled: true
rest_polling:
  enabled: true
monitoring:
  enabled: false
initial_balances:
  mexc:
    USDT: 20.0
  coinex:
    USDT: 20.0
mexc:
  api_key: ""
  api_secret: ""
coinex:
  api_key: ""
  api_secret: ""
live_preflight:
  enabled: true
  target_mode: small_live_taker_taker
  exchanges: [mexc, coinex]
  market_type: Spot
  symbols: [BTCUSDT]
  max_live_notional_per_trade: 5
  max_total_live_notional: 20
{live_preflight_extra}
"#
    )
}

#[test]
fn cli_preflight_should_exit_nonzero_on_fail() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("blocked.yml");
    fs::write(&path, base_config("")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rustcta"))
        .arg("--preflight")
        .arg("--config")
        .arg(&path)
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn cli_preflight_should_exit_zero_when_required_checks_pass() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("pass.yml");
    fs::write(
        &path,
        base_config(
            r#"
  require_monitoring_enabled: false
  require_recorder_enabled: false
  require_websocket_fresh: false
  require_fee_model: false
  require_disabled_registry: false
  require_kill_switch: false
  require_balances: false
  require_symbol_rules: false
  require_order_validation: false
  require_api_key_read_permission: false
  require_withdraw_permission_absent: false
"#,
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rustcta"))
        .arg("--preflight")
        .arg("--config")
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}
