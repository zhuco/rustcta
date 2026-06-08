use serde_json::Value;
use std::path::PathBuf;
use std::process::Command;

fn tools_ops_bin() -> &'static str {
    env!("CARGO_BIN_EXE_rustcta-tools-ops")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tools directory")
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn run_tools_ops(args: &[&str]) -> std::process::Output {
    Command::new(tools_ops_bin())
        .current_dir(workspace_root())
        .args(args)
        .output()
        .expect("rustcta-tools-ops should run")
}

fn assert_success(output: &std::process::Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn default_command_should_verify_retired_src() {
    let output = run_tools_ops(&[]);
    assert_success(&output, "default verify-retired-src");

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("default command should emit json");
    assert_eq!(report["retired"], true);
}

#[test]
fn verify_retired_src_should_stay_clean() {
    let output = run_tools_ops(&["verify-retired-src"]);
    assert_success(&output, "verify-retired-src");

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("verify-retired-src should emit json");
    assert_eq!(report["src_dir"], "src");
    assert_eq!(report["exists"], false);
    assert_eq!(report["retired"], true);
}

#[test]
fn migrated_command_help_should_not_drift() {
    for (args, expected) in [
        (
            &["smart-money", "binance-collector", "--help"][..],
            "Usage: rustcta-tools-ops smart-money binance-collector",
        ),
        (
            &["smart-money", "hyperliquid-wallet-ingestion", "--help"][..],
            "Usage: rustcta-tools-ops smart-money hyperliquid-wallet-ingestion",
        ),
        (
            &["smart-money", "portfolio-service", "--help"][..],
            "Usage: rustcta-tools-ops smart-money portfolio-service",
        ),
        (
            &["symbols", "gateio-bitget-spot", "--help"][..],
            "Usage: rustcta-tools-ops symbols gateio-bitget-spot",
        ),
        (
            &["reporter", "trend", "--help"][..],
            "Usage: rustcta-tools-ops reporter trend",
        ),
        (
            &["reporter", "account-position", "render", "--help"][..],
            "Usage: rustcta-tools-ops reporter account-position render",
        ),
        (
            &["ws-proxy-probe", "--help"][..],
            "Usage: rustcta-tools-ops ws-proxy-probe",
        ),
        (
            &["probe", "ws-proxy", "--help"][..],
            "Usage: rustcta-tools-ops probe ws-proxy",
        ),
        (
            &["probe", "hyperliquid-self-test", "--help"][..],
            "Usage: rustcta-tools-ops probe hyperliquid-self-test",
        ),
        (
            &["probe", "funding-arb-observe", "--help"][..],
            "Usage: rustcta-tools-ops probe funding-arb-observe",
        ),
        (
            &["probe", "cross-arb-ws-opportunity", "--help"][..],
            "Usage: rustcta-tools-ops probe cross-arb-ws-opportunity",
        ),
        (
            &["canary", "exchange-order", "--help"][..],
            "Usage: rustcta-tools-ops canary exchange-order",
        ),
        (
            &["canary", "bitget-perp-order", "--help"][..],
            "Usage: rustcta-tools-ops canary bitget-perp-order",
        ),
        (
            &["canary", "bitget-spot-order", "--help"][..],
            "Usage: rustcta-tools-ops canary bitget-spot-order",
        ),
        (
            &["audit", "cross-arb-account", "--help"][..],
            "Usage: rustcta-tools-ops audit cross-arb-account",
        ),
        (
            &["audit", "cross-arb-fee", "--help"][..],
            "Usage: rustcta-tools-ops audit cross-arb-fee",
        ),
        (
            &["admin", "cross-arb-order", "--help"][..],
            "Usage: rustcta-tools-ops admin cross-arb-order",
        ),
    ] {
        let output = run_tools_ops(args);
        assert_success(&output, &args.join(" "));

        let stdout = stdout(&output);
        assert!(
            stdout.contains(expected),
            "{} help output should contain {expected:?}\nstdout:\n{stdout}",
            args.join(" ")
        );
    }
}

#[test]
fn ws_proxy_probe_help_should_include_region_dimension() {
    let output = run_tools_ops(&["probe", "ws-proxy", "--help"]);
    assert_success(&output, "probe ws-proxy --help");

    let stdout = stdout(&output);
    assert!(stdout.contains("--region"));
    assert!(stdout.contains("--exchange"));
}

#[test]
fn live_canary_safety_plan_should_remain_non_mutating() {
    let output = run_tools_ops(&["canary", "exchange-order"]);
    assert_success(&output, "canary exchange-order");

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("safety plan should emit json");
    assert_eq!(report["legacy_binary"], "exchange_order_canary");
    assert_eq!(report["gate_mode"], "dry_run_plan_only");
    assert_eq!(report["execute_requested"], false);
    assert_eq!(report["request"]["exchange"], "bitget");
    assert_eq!(report["request"]["symbol"], "DOGE/USDT");
    assert_eq!(report["planned_side_effects"], serde_json::json!([]));
    assert!(report["output_fields_preserved"]
        .as_array()
        .expect("fields array")
        .iter()
        .any(|field| field == "open_ack"));
}

#[test]
fn hyperliquid_self_test_plan_should_remain_secret_free() {
    let output = run_tools_ops(&["probe", "hyperliquid-self-test"]);
    assert_success(&output, "probe hyperliquid-self-test");

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("hyperliquid plan should emit json");
    assert_eq!(report["legacy_binary"], "hyperliquid_self_test");
    assert_eq!(report["gate_mode"], "secret_free_plan_only");
    assert_eq!(report["execute_requested"], false);
    assert_eq!(report["live_order_confirmed"], false);
    assert_eq!(report["request"]["symbol"], "ETH/USDC");
    assert_eq!(report["planned_side_effects"], serde_json::json!([]));
    assert!(report["safety_checks"]
        .as_array()
        .expect("safety checks")
        .iter()
        .any(|check| check == "tools/ops does not read Hyperliquid wallet or private key values"));
}

#[test]
fn observe_probe_plans_should_remain_non_mutating() {
    let output = run_tools_ops(&["probe", "funding-arb-observe"]);
    assert_success(&output, "probe funding-arb-observe");
    let report: Value =
        serde_json::from_slice(&output.stdout).expect("funding observe plan should emit json");
    assert_eq!(report["legacy_binary"], "funding_arb_observe");
    assert_eq!(report["gate_mode"], "dry_run_plan_only");
    assert_eq!(report["planned_side_effects"], serde_json::json!([]));
    assert_eq!(report["request"]["live_order_access"], "disabled");

    let output = run_tools_ops(&["probe", "cross-arb-ws-opportunity"]);
    assert_success(&output, "probe cross-arb-ws-opportunity");
    let report: Value =
        serde_json::from_slice(&output.stdout).expect("ws opportunity plan should emit json");
    assert_eq!(report["legacy_binary"], "cross_arb_ws_opportunity_probe");
    assert_eq!(report["gate_mode"], "dry_run_plan_only");
    assert_eq!(report["planned_side_effects"], serde_json::json!([]));
    assert_eq!(report["request"]["network_access"], "disabled");
}

#[test]
fn safety_gate_failures_should_not_require_live_config_or_network() {
    let output = run_tools_ops(&[
        "canary",
        "bitget-perp-order",
        "--config",
        "missing.yml",
        "--execute",
    ]);
    assert!(
        !output.status.success(),
        "unconfirmed execute should fail\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--execute requires --confirm-live-order"));

    let output = run_tools_ops(&[
        "probe",
        "hyperliquid-self-test",
        "--run-orders",
        "--symbol",
        "ETH/USDC",
    ]);
    assert!(
        !output.status.success(),
        "unconfirmed hyperliquid order path should fail\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--run-orders requires --execute --confirm-live-order"));

    let output = run_tools_ops(&[
        "admin",
        "cross-arb-order",
        "--config",
        "missing.yml",
        "--action",
        "cancel",
        "--symbol",
        "BTC/USDT",
        "--exchange-order-id",
        "123",
    ]);
    assert!(
        !output.status.success(),
        "unconfirmed cancel should fail\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--action cancel requires --execute --confirm-cancel"));
}

#[test]
fn smart_money_summary_commands_should_stay_dry_run_and_network_free() {
    for (args, expected) in [
        (
            &[
                "smart-money",
                "binance-collector",
                "--config",
                "config/smart_money.yml",
            ][..],
            "no Binance network connections were opened",
        ),
        (
            &[
                "smart-money",
                "hyperliquid-wallet-ingestion",
                "--config",
                "config/smart_money.yml",
            ][..],
            "no Hyperliquid network connections were opened",
        ),
        (
            &[
                "smart-money",
                "portfolio-service",
                "--config",
                "config/smart_money.yml",
            ][..],
            "no portfolio orders or network connections were created",
        ),
    ] {
        let output = run_tools_ops(args);
        assert_success(&output, &args.join(" "));

        let stdout = stdout(&output);
        assert!(
            stdout.contains(expected),
            "{} should contain {expected:?}\nstdout:\n{stdout}",
            args.join(" ")
        );
    }
}
