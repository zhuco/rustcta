use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

fn run_industrial(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rustcta-industrial"))
        .args(args)
        .current_dir(workspace_root())
        .output()
        .expect("run rustcta-industrial")
}

const SUPERVISOR_READINESS_ARGS: &[&str] =
    &["supervisor", "readiness", "--spec-dir", "config/supervisor"];

fn assert_success(args: &[&str], stdout_contains: &[&str]) {
    let output = run_industrial(args);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "rustcta-industrial {args:?} failed with status {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );

    for expected in stdout_contains {
        assert!(
            stdout.contains(expected),
            "rustcta-industrial {args:?} stdout did not contain {expected:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }
}

#[test]
fn command_tree_help_should_cover_operator_surfaces() {
    assert_success(
        &["--help"],
        &["Commands:", "migration", "ledger", "supervisor", "ops"],
    );
    assert_success(&["migration", "--help"], &["verify-retired-src"]);
    assert_success(&["ledger", "--help"], &["validate", "summary"]);
    assert_success(
        &["supervisor", "--help"],
        &[
            "print-legacy-spec",
            "readiness",
            "validate-registry",
            "validate-spec",
        ],
    );
    assert_success(&["ops", "--help"], &["smart-money", "reporter", "symbols"]);
    assert_success(&["cross-arb", "--help"], &["preflight", "observe"]);
    assert_success(
        &["cross-arb", "preflight", "--help"],
        &[
            "Usage:",
            "preflight",
            "--config",
            "--private",
            "--private-symbol-sample",
        ],
    );
    assert_success(
        &["cross-arb", "observe", "--help"],
        &["Usage:", "observe", "--config", "--request-timeout-ms"],
    );
}

#[test]
fn safe_command_tree_should_pass_binary_contract_smoke() {
    let _ = fs::remove_file("/tmp/rustcta-missing-events.jsonl");
    let _ = fs::remove_file("/tmp/rustcta-missing-registry.json");

    assert_success(&["doctor"], &["industrial workspace scaffold is installed"]);
    assert_success(
        &["migration", "verify-retired-src"],
        &["\"src_dir\": \"src\"", "\"retired\": true"],
    );
    assert_success(
        &[
            "ledger",
            "validate",
            "--path",
            "/tmp/rustcta-missing-events.jsonl",
        ],
        &["\"valid\": true", "\"event_count\": 0"],
    );
    assert_success(
        &[
            "supervisor",
            "validate-registry",
            "--path",
            "/tmp/rustcta-missing-registry.json",
        ],
        &["\"valid\": true", "\"process_count\": 0"],
    );
    assert_success(
        SUPERVISOR_READINESS_ARGS,
        &[
            "\"valid\": true",
            "\"spot_spot_live_dry_run\"",
            "\"operator_gated\": true",
        ],
    );
    assert_success(
        &[
            "ops",
            "smart-money",
            "binance-collector",
            "--config",
            "config/smart_money.yml",
        ],
        &["dry-run", "Binance"],
    );
    assert_success(
        &["ops", "reporter", "account-position", "render", "--help"],
        &["Usage:", "render"],
    );
    assert_success(
        &["ops", "symbols", "gateio-bitget-spot", "--help"],
        &["Usage:", "gateio-bitget-spot"],
    );
    assert_success(
        &["cross-arb", "preflight", "--help"],
        &["Usage:", "preflight"],
    );
    assert_success(&["cross-arb", "observe", "--help"], &["Usage:", "observe"]);
}

#[test]
fn cross_arb_preflight_bridge_should_emit_offline_plan_without_network_paths() {
    let output = run_industrial(&[
        "cross-arb",
        "preflight",
        "--config",
        "config/cross_exchange_arbitrage_usdt.yml",
        "--private-readonly",
        "--timeout-ms",
        "123",
        "--private-symbol-sample",
        "2",
        "--public-orderbook-sample",
        "3",
        "--full-symbol-checks",
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "rustcta-industrial cross-arb preflight failed with status {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("preflight bridge should emit json");
    assert_eq!(report["command"], "cross-arb preflight");
    assert_eq!(report["legacy_binary"], "cross_arb_preflight");
    assert_eq!(report["network_access"], "disabled");
    assert_eq!(report["live_order_access"], "disabled");
    assert_eq!(report["private_readonly"], true);
    assert_eq!(report["timeout_ms"], 123);
    let legacy_args = report["legacy_args"]
        .as_array()
        .expect("legacy_args should be an array");
    for expected in [
        "--config",
        "config/cross_exchange_arbitrage_usdt.yml",
        "--private",
        "--full-symbol-checks",
    ] {
        assert!(
            legacy_args.iter().any(|arg| arg == expected),
            "preflight bridge legacy args missing {expected}: {legacy_args:#?}"
        );
    }
}

#[test]
fn cross_arb_observe_bridge_should_emit_offline_contract_without_network_paths() {
    let output = run_industrial(&[
        "cross-arb",
        "observe",
        "--config",
        "config/cross_exchange_arbitrage_usdt.yml",
        "--request-timeout-ms",
        "456",
        "--max-symbols",
        "2",
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "rustcta-industrial cross-arb observe failed with status {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("observe bridge should emit json");
    assert_eq!(report["command"], "cross-arb observe");
    assert_eq!(report["legacy_binary"], "cross_arb_observe");
    assert_eq!(report["network_access"], "disabled");
    assert_eq!(report["live_order_access"], "disabled");
    assert_eq!(report["request_timeout_ms"], 456);
    assert_eq!(report["max_symbols"], 2);
    assert_eq!(report["summary"]["mode"], "observe");
    assert_eq!(report["summary"]["books_loaded"], 0);
    assert_eq!(report["summary"]["funding_loaded"], 0);
    let output_fields = report["output_fields_preserved"]
        .as_array()
        .expect("output_fields_preserved should be an array");
    for expected in ["opportunities", "errors", "exchanges_seen"] {
        assert!(
            output_fields.iter().any(|field| field == expected),
            "observe bridge output fields missing {expected}: {output_fields:#?}"
        );
    }
    let legacy_args = report["legacy_args"]
        .as_array()
        .expect("legacy_args should be an array");
    for expected in [
        "--config",
        "config/cross_exchange_arbitrage_usdt.yml",
        "--request-timeout-ms",
        "456",
        "--max-symbols",
        "2",
    ] {
        assert!(
            legacy_args.iter().any(|arg| arg == expected),
            "observe bridge legacy args missing {expected}: {legacy_args:#?}"
        );
    }
}

#[test]
fn supervisor_readiness_should_report_checked_in_specs_in_run_order() {
    let output = run_industrial(SUPERVISOR_READINESS_ARGS);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "rustcta-industrial supervisor readiness failed with status {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );

    let report: Value = serde_json::from_slice(&output.stdout).expect("readiness should emit json");
    assert_eq!(report["valid"], true);
    assert_eq!(report["spec_dir"], "config/supervisor");
    assert_eq!(report["spec_count"], 5);

    let recommended_start_order = report["recommended_start_order"]
        .as_array()
        .expect("recommended_start_order should be an array")
        .iter()
        .map(|strategy_id| {
            strategy_id
                .as_str()
                .expect("recommended start order item should be a string")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        recommended_start_order,
        [
            "trend_report",
            "account_position_reporter",
            "cross_arb_live",
            "funding_arb_live",
            "spot_spot_live_dry_run",
        ]
    );

    let specs = report["specs"]
        .as_array()
        .expect("specs should be an array");
    let strategy_ids = specs
        .iter()
        .map(|spec| {
            spec["strategy_id"]
                .as_str()
                .expect("spec strategy_id should be a string")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        strategy_ids,
        [
            "trend_report",
            "account_position_reporter",
            "cross_arb_live",
            "funding_arb_live",
            "spot_spot_live_dry_run",
        ]
    );

    for spec in specs {
        assert_eq!(spec["valid"], true, "{spec:#}");
        assert_eq!(spec["config_exists"], true, "{spec:#}");
    }

    let spot_spot = specs
        .iter()
        .find(|spec| spec["strategy_id"] == "spot_spot_live_dry_run")
        .expect("spot_spot readiness entry");
    assert_eq!(spot_spot["operator_gated"], true);
    assert_eq!(spot_spot["first_run"], false);
    let safety_notes = spot_spot["safety_notes"]
        .as_array()
        .expect("spot_spot safety_notes should be an array");
    for expected in [
        "live_dry_run.submit_orders=false:true",
        "small_live_gate.explicit_live_confirmation=true:true",
        "live_preflight.max_live_notional_per_trade=3.6:true",
        "monitoring.http_enabled=false:true",
        "kill_switch.allow_live_orders=true:true",
        "inventory_rebalance.allow_lossy_rebalance_when_blocked=true:true",
        "inline_credentials=false:true",
    ] {
        assert!(
            safety_notes.iter().any(|note| note == expected),
            "spot_spot readiness missing safety note {expected}"
        );
    }
}
