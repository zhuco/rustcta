mod support;

use rustcta::exchanges::{GateIoSpotClient, GateIoSpotConfig};
use std::sync::Arc;

use support::live_readonly::{
    has_gateio_credentials, validate_authenticated_readonly, validate_public_websocket,
    write_report, LiveReadOnlyExchangeReport, LiveReadonlyTestConfig, MutationCallDetector,
    ReadOnlyGuardClient,
};

#[tokio::test]
#[ignore]
async fn live_readonly_gateio_should_match_control_plane_semantics_without_mutations() {
    let Some(config) = LiveReadonlyTestConfig::from_env() else {
        return;
    };

    let detector = Arc::new(MutationCallDetector::default());
    let gateio_config = GateIoSpotConfig {
        dry_run: true,
        log_raw_messages: false,
        enabled_symbols: vec![config.symbol.clone()],
        ..GateIoSpotConfig::default()
    };
    let client = ReadOnlyGuardClient::new(GateIoSpotClient::new(gateio_config), detector.clone());
    let mut report = LiveReadOnlyExchangeReport::new("gateio", &config);

    if has_gateio_credentials() {
        validate_authenticated_readonly(&client, &config, &mut report).await;
    } else {
        report.credential_status = "missing_credentials_authenticated_checks_skipped".to_string();
        report.warnings.push(
            "Gate.io credential environment variables missing; authenticated checks skipped".into(),
        );
    }

    validate_public_websocket(&client, &config, &mut report).await;
    report.mutation_calls_detected = detector.total_mutations();
    write_report(&report, &config);
    report.assert_no_critical_errors();
}
