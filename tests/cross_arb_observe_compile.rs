use std::process::Command;

#[test]
fn cross_arb_observe_should_expose_cli_without_network() {
    let output = Command::new(env!("CARGO_BIN_EXE_cross_arb_observe"))
        .arg("--help")
        .output()
        .expect("run cross_arb_observe --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cross_arb_observe"));
    assert!(stdout.contains("--config"));
}
