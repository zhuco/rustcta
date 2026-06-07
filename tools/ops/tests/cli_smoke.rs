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
    assert!(migrations
        .iter()
        .all(|migration| migration["target"] == "tool_ops"));
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
    ] {
        let output = run_tools_ops(args);
        assert_success(&output, &args.join(" "));

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains(expected),
            "{} help output should contain {expected:?}\nstdout:\n{stdout}",
            args.join(" ")
        );
    }
}
