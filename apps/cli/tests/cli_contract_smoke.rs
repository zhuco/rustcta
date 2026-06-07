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
fn safe_command_tree_should_pass_binary_contract_smoke() {
    let _ = fs::remove_file("/tmp/rustcta-missing-events.jsonl");
    let _ = fs::remove_file("/tmp/rustcta-missing-registry.json");

    assert_success(&["doctor"], &["industrial workspace scaffold is installed"]);
    assert_success(
        &["migration", "legacy-bin-plan", "--target", "tool-ops"],
        &["account_position_reporter.rs", "tool_ops"],
    );
    assert_success(
        &[
            "migration",
            "verify-legacy-bins",
            "--src-bin-dir",
            "src/bin",
        ],
        &["unclassified_bins", "stale_migrations"],
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
