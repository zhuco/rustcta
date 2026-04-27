use chrono::{Duration, TimeZone, Utc};
use rustcta::backtest::data::depth_dataset::DepthDatasetWriter;
use rustcta::backtest::schema::DepthDeltaEvent;
use tempfile::tempdir;

fn sample_depth_delta(
    logical_ts_ms: i64,
    first_update_id: u64,
    final_update_id: u64,
    bid_price: f64,
    ask_price: f64,
) -> DepthDeltaEvent {
    let logical_ts = Utc.timestamp_millis_opt(logical_ts_ms).unwrap();
    DepthDeltaEvent {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        exchange_ts: logical_ts + Duration::milliseconds(5),
        logical_ts,
        first_update_id,
        final_update_id,
        bids: vec![[bid_price, 2.0]],
        asks: vec![[ask_price, 3.0]],
    }
}

#[test]
fn persists_manifest_and_chronological_deduped_depth_deltas() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = DepthDatasetWriter::new(temp_dir.path());

    let deltas = vec![
        sample_depth_delta(1_711_929_780_000, 13, 13, 100.0, 101.0),
        sample_depth_delta(1_711_929_660_000, 11, 11, 98.0, 99.0),
        sample_depth_delta(1_711_929_720_000, 12, 12, 99.0, 100.0),
        sample_depth_delta(1_711_929_720_500, 12, 12, 99.5, 100.5),
    ];

    let output = writer
        .write_binance_futures_depth_deltas("BTC/USDT", &deltas)
        .expect("depth dataset write should succeed");

    assert!(output.manifest_path.exists(), "manifest should exist");
    assert!(output.data_path.exists(), "data file should exist");
    assert!(
        output
            .data_path
            .to_string_lossy()
            .contains("symbol=BTC_USDT"),
        "dataset path should sanitize standard symbols for filesystem safety"
    );

    let persisted = std::fs::read_to_string(&output.data_path).expect("read data file");
    let rows: Vec<DepthDeltaEvent> =
        serde_json::from_str(&persisted).expect("parse persisted depth deltas");
    assert_eq!(
        rows.len(),
        3,
        "duplicate final update ids should be collapsed"
    );

    let update_ids: Vec<u64> = rows.iter().map(|row| row.final_update_id).collect();
    assert_eq!(update_ids, vec![11, 12, 13]);

    let logical_times: Vec<_> = rows.iter().map(|row| row.logical_ts).collect();
    assert!(logical_times.windows(2).all(|pair| pair[0] < pair[1]));

    let manifest = std::fs::read_to_string(&output.manifest_path).expect("read manifest");
    let manifest_json: serde_json::Value =
        serde_json::from_str(&manifest).expect("parse manifest json");

    assert_eq!(manifest_json["exchange"], "binance");
    assert_eq!(manifest_json["market"], "futures");
    assert_eq!(manifest_json["symbol"], "BTC/USDT");
    assert_eq!(manifest_json["row_count"], 3);
}
