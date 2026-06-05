mod support;

use rustcta::exchanges::{BitgetSpotClient, BitgetSpotConfig, GateIoSpotClient, GateIoSpotConfig};
use std::sync::Arc;

use support::live_readonly::{
    has_bitget_credentials, has_gateio_credentials, run_runtime_publisher_session, write_report,
    LiveReadonlyTestConfig, MutationCallDetector, ReadOnlyGuardClient,
};

#[tokio::test]
#[ignore]
async fn live_runtime_publisher_should_poll_readonly_sources_and_persist_snapshots() {
    let Some(config) = LiveReadonlyTestConfig::from_env() else {
        return;
    };

    let gateio_detector = Arc::new(MutationCallDetector::default());
    let bitget_detector = Arc::new(MutationCallDetector::default());

    let gateio = has_gateio_credentials().then(|| {
        let cfg = GateIoSpotConfig {
            dry_run: true,
            log_raw_messages: false,
            enabled_symbols: vec![config.symbol.clone()],
            ..GateIoSpotConfig::default()
        };
        ReadOnlyGuardClient::new(GateIoSpotClient::new(cfg), gateio_detector.clone())
    });
    let bitget = has_bitget_credentials().then(|| {
        let cfg = BitgetSpotConfig {
            dry_run: true,
            log_raw_messages: false,
            enabled_symbols: vec![config.symbol.clone()],
            ..BitgetSpotConfig::default()
        };
        ReadOnlyGuardClient::new(BitgetSpotClient::new(cfg), bitget_detector.clone())
    });

    if gateio.is_none() && bitget.is_none() {
        eprintln!("no Gate.io or Bitget credentials found; runtime publisher live test skipped");
        return;
    }

    let mut report = run_runtime_publisher_session(gateio, bitget, &config).await;
    report.mutation_calls_detected =
        gateio_detector.total_mutations() + bitget_detector.total_mutations();

    write_report(&report, &config);
    report.assert_no_critical_errors();
}
