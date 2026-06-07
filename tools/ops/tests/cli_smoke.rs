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
fn legacy_bin_plan_should_include_tools_migrations() {
    let output = run_tools_ops(&["legacy-bin-plan", "--target", "tools"]);
    assert_success(&output, "legacy-bin-plan --target tools");

    let migrations: Vec<Value> =
        serde_json::from_slice(&output.stdout).expect("legacy-bin-plan should emit json");
    let sources = migrations
        .iter()
        .map(|migration| {
            migration["source"]
                .as_str()
                .expect("migration source should be a string")
        })
        .collect::<Vec<_>>();

    assert!(sources.contains(&"trend_report.rs"));
    assert!(sources.contains(&"account_position_reporter.rs"));
    assert!(sources.contains(&"ws_proxy_probe.rs"));
    assert!(sources.contains(&"gateio_bitget_spot_symbols.rs"));
    assert!(sources.contains(&"smart_money_binance_collector.rs"));
    assert!(sources.contains(&"smart_money_hyperliquid_wallet_ingestion.rs"));
    assert!(sources.contains(&"smart_money_portfolio_service.rs"));
    assert!(migrations
        .iter()
        .all(|migration| migration["target"] == "tool_ops"));
    assert!(migrations
        .iter()
        .any(|migration| migration["compatibility"] == "alias"));
    assert!(migrations.iter().any(|migration| migration["new_command"]
        .as_str()
        .is_some_and(|command| command.contains("rustcta-tools-ops"))));
    assert!(migrations
        .iter()
        .all(|migration| migration["retirement_milestone"]
            .as_str()
            .is_some_and(|milestone| !milestone.is_empty())));
}

#[test]
fn verify_legacy_bins_should_stay_clean() {
    let output = run_tools_ops(&["verify-legacy-bins", "--src-bin-dir", "src/bin"]);
    assert_success(&output, "verify-legacy-bins --src-bin-dir src/bin");

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("verify-legacy-bins should emit json");
    assert_eq!(report["unclassified_bins"], serde_json::json!([]));
    assert_eq!(report["stale_migrations"], serde_json::json!([]));
    assert!(report["classified_bins"]
        .as_array()
        .expect("classified_bins should be an array")
        .iter()
        .any(|source| source == "ws_proxy_probe.rs"));
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

#[test]
fn legacy_tool_wrappers_should_forward_to_expected_tools_ops_subcommands() {
    for (source, expected_subcommand) in [
        (
            "src/bin/smart_money_binance_collector.rs",
            "[\"smart-money\", \"binance-collector\"]",
        ),
        (
            "src/bin/smart_money_hyperliquid_wallet_ingestion.rs",
            "[\"smart-money\", \"hyperliquid-wallet-ingestion\"]",
        ),
        (
            "src/bin/smart_money_portfolio_service.rs",
            "[\"smart-money\", \"portfolio-service\"]",
        ),
        ("src/bin/ws_proxy_probe.rs", "[\"ws-proxy-probe\"]"),
        (
            "src/bin/gateio_bitget_spot_symbols.rs",
            "[\"symbols\", \"gateio-bitget-spot\"]",
        ),
        ("src/bin/trend_report.rs", "[\"reporter\", \"trend\"]"),
    ] {
        let source_path = workspace_root().join(source);
        let source = std::fs::read_to_string(&source_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", source_path.display()));
        assert!(
            source.contains("legacy_tools_ops_shim::run_tools_ops"),
            "{} should use the shared tools/ops shim",
            source_path.display()
        );
        assert!(
            source.contains(expected_subcommand),
            "{} should forward to {expected_subcommand}",
            source_path.display()
        );
    }
}
