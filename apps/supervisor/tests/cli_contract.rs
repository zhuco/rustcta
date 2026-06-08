use serde_json::Value;
use std::path::PathBuf;
use std::process::Command;

fn supervisor_bin() -> &'static str {
    env!("CARGO_BIN_EXE_rustcta-supervisor")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("apps directory")
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn print_legacy_spec_should_generate_basic_fields() {
    let output = Command::new(supervisor_bin())
        .args([
            "--print-legacy-spec",
            "trend-report",
            "--strategy-id",
            "contract_trend_report",
            "--run-id",
            "contract",
            "--tenant-id",
            "local",
        ])
        .output()
        .expect("rustcta-supervisor should run");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let spec: Value =
        serde_json::from_slice(&output.stdout).expect("print-legacy-spec should emit json");
    assert_eq!(spec["schema_version"], 1);
    assert_eq!(spec["strategy_id"], "contract_trend_report");
    assert_eq!(spec["run_id"], "contract");
    assert_eq!(spec["tenant_id"], "local");
    assert_eq!(spec["strategy_kind"], "trend_report");
    assert_eq!(spec["command"], "cargo");
    assert!(spec["args"]
        .as_array()
        .expect("args should be an array")
        .iter()
        .any(|arg| arg == "trend_report"));
}

#[test]
fn validate_spec_should_accept_checked_in_small_runtime_specs() {
    for (spec_name, strategy_id, strategy_kind, arg_count) in [
        (
            "cross_arb_live.spec.json",
            "cross_arb_live",
            "cross_exchange_arbitrage",
            6,
        ),
        (
            "funding_arb_live.spec.json",
            "funding_arb_live",
            "funding_arbitrage",
            7,
        ),
        (
            "spot_spot_live_dry_run.spec.json",
            "spot_spot_live_dry_run",
            "spot_spot_taker_arbitrage",
            8,
        ),
        ("trend_report.spec.json", "trend_report", "trend_report", 6),
        (
            "account_position_reporter.spec.json",
            "account_position_reporter",
            "account_position_report",
            6,
        ),
    ] {
        let spec_path = workspace_root().join("config/supervisor").join(spec_name);
        let output = Command::new(supervisor_bin())
            .arg("--validate-spec")
            .arg(&spec_path)
            .output()
            .expect("rustcta-supervisor should run");

        assert!(
            output.status.success(),
            "stderr for {spec_name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let report: Value =
            serde_json::from_slice(&output.stdout).expect("validate-spec should emit json");
        assert_eq!(report["valid"], true, "{spec_name}");
        assert_eq!(report["schema_version"], 1, "{spec_name}");
        assert_eq!(report["strategy_id"], strategy_id, "{spec_name}");
        assert_eq!(report["strategy_kind"], strategy_kind, "{spec_name}");
        assert_eq!(report["arg_count"], arg_count, "{spec_name}");
        assert_eq!(report["working_dir_configured"], true, "{spec_name}");
        assert_eq!(report["log_configured"], true, "{spec_name}");

        let raw = std::fs::read_to_string(&spec_path).expect("checked-in spec should be readable");
        let spec: Value = serde_json::from_str(&raw).expect("checked-in spec should be json");
        let config_path = spec["config_path"]
            .as_str()
            .expect("checked-in spec should contain config_path");
        assert!(
            workspace_root().join(config_path).exists(),
            "{spec_name} config_path should exist: {config_path}"
        );
    }
}

#[test]
fn spot_spot_live_dry_run_config_should_remain_manual_until_spec_safety_is_tighter() {
    let config_path =
        workspace_root().join("config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml");
    let config = std::fs::read_to_string(&config_path)
        .expect("spot_spot live-dry-run config should be readable");

    assert!(
        config
            .contains("\n  enabled: true\n  build_order_requests: true\n  submit_orders: false\n"),
        "spot_spot live-dry-run must build requests without submitting them"
    );
    assert!(
        config.contains("\n  submit_orders: false\n"),
        "spot_spot live-dry-run must not submit orders"
    );
    assert!(
        config.contains("\n  explicit_live_confirmation: true\n"),
        "spot_spot small-live gate must require explicit confirmation"
    );
    assert!(
        config.contains("\n  max_live_notional_per_trade: 3.6\n"),
        "spot_spot preflight must keep the active live trade cap small"
    );
    assert!(
        config.contains("\n  max_total_live_notional: 50\n"),
        "spot_spot preflight must keep the total live exposure cap bounded"
    );
    assert!(
        config.contains("\n  http_enabled: false\n"),
        "spot_spot live-dry-run monitoring HTTP must remain disabled"
    );
    assert!(
        config.contains("\n  expose_publicly: false\n"),
        "spot_spot monitoring must not be public"
    );
    assert!(
        config.contains("\n  require_write_auth: true\n"),
        "spot_spot symbol-control writes must require authorization"
    );
    assert!(
        config.contains("\nlive_trading_enabled: true\n"),
        "spot_spot config still uses live runtime mode"
    );
    assert!(
        config.contains("\ndry_run: false\n"),
        "spot_spot config is not a global dry_run strategy config"
    );
    assert!(
        config.contains("\n  allow_live_orders: true\n"),
        "spot_spot kill switch still allows live orders at config level"
    );
    assert!(
        config.contains("\n  require_api_key_trade_permission: true\n"),
        "spot_spot preflight still requires trade-permission credentials"
    );
    assert!(
        config.contains("\n  require_withdraw_permission_absent: true\n"),
        "spot_spot preflight must reject withdrawal-capable credentials"
    );
    assert!(
        config.contains("\n  require_private_stream: false\n"),
        "spot_spot readiness must report that private stream is not required"
    );
    assert!(
        config.contains("\n  allow_rest_order_polling_fallback: true\n"),
        "spot_spot readiness must report REST order polling fallback"
    );
    assert!(
        config.contains("\n  allow_auto_initial_entry: false\n"),
        "spot_spot inventory rebalance must not auto-start initial entry"
    );
    assert!(
        config.contains("\n  allow_auto_exit: false\n"),
        "spot_spot inventory rebalance must not auto-exit"
    );
    assert!(
        config.contains("\n  allow_market_rebalance: true\n"),
        "spot_spot readiness must report market rebalance is enabled"
    );
    assert!(
        config.contains("\n  allow_lossy_rebalance_when_blocked: true\n"),
        "spot_spot readiness must report blocked lossy rebalance allowance"
    );
    assert!(
        !config.contains("\napi_key:") && !config.contains("\n  api_key:"),
        "spot_spot checked-in config must not inline API keys"
    );
    assert!(
        !config.contains("\napi_secret:") && !config.contains("\n  api_secret:"),
        "spot_spot checked-in config must not inline API secrets"
    );
    assert!(
        !config.contains("\npassphrase:") && !config.contains("\n  passphrase:"),
        "spot_spot checked-in config must not inline passphrases"
    );
    assert!(
        !config.contains(".env") && !config.contains("~/"),
        "spot_spot checked-in config should avoid local credential or home-directory paths"
    );

    let spec_path = workspace_root()
        .join("config/supervisor")
        .join("spot_spot_live_dry_run.spec.json");
    let raw =
        std::fs::read_to_string(&spec_path).expect("spot_spot checked-in spec should be readable");
    let spec: Value = serde_json::from_str(&raw).expect("spot_spot checked-in spec should be json");
    assert_eq!(spec["strategy_id"], "spot_spot_live_dry_run");
    assert_eq!(spec["strategy_kind"], "spot_spot_taker_arbitrage");
    assert_eq!(
        spec["config_path"],
        "config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml"
    );
    assert!(
        spec["args"]
            .as_array()
            .expect("spot_spot args should be an array")
            .iter()
            .any(|arg| arg == "spot_spot_taker_arbitrage"),
        "spot_spot checked-in spec should launch the explicit spot_spot strategy"
    );
}

#[test]
fn validate_registry_should_accept_missing_registry_as_empty() {
    let missing_registry = std::env::temp_dir().join(format!(
        "rustcta-supervisor-missing-registry-{}.json",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&missing_registry);

    let output = Command::new(supervisor_bin())
        .arg("--registry-path")
        .arg(&missing_registry)
        .arg("--validate-registry")
        .output()
        .expect("rustcta-supervisor should run");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("validate-registry should emit json");
    assert_eq!(report["valid"], true);
    assert_eq!(report["process_count"], 0);
    assert_eq!(report["running"], 0);
    assert_eq!(report["failed"], 0);
    assert_eq!(report["stopped"], 0);
}
