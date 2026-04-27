use chrono::{TimeZone, Utc};
use rustcta::backtest::data::depth_capture::{
    import_binance_futures_depth_capture, DepthCaptureSession, DepthCaptureWriter,
    RawDepthDeltaRecord, RawDepthSnapshot,
};
use rustcta::backtest::data::depth_dataset::DepthDatasetReader;
use tempfile::tempdir;

fn sample_snapshot() -> RawDepthSnapshot {
    RawDepthSnapshot {
        last_update_id: 100,
        timestamp: Utc.timestamp_millis_opt(1_711_929_700_000).unwrap(),
        bids: vec![[100.0, 2.0], [99.0, 3.0]],
        asks: vec![[101.0, 2.0], [102.0, 3.0]],
    }
}

fn sample_delta(
    logical_ts_ms: i64,
    first_update_id: u64,
    final_update_id: u64,
    previous_final_update_id: Option<u64>,
    bids: Vec<[f64; 2]>,
    asks: Vec<[f64; 2]>,
) -> RawDepthDeltaRecord {
    let logical_ts = Utc.timestamp_millis_opt(logical_ts_ms).unwrap();
    RawDepthDeltaRecord {
        exchange_ts: logical_ts,
        logical_ts,
        first_update_id,
        final_update_id,
        previous_final_update_id,
        bids,
        asks,
    }
}

#[test]
fn imports_raw_capture_into_normalized_depth_dataset() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = DepthCaptureWriter::new(temp_dir.path());

    let session = DepthCaptureSession {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        stream: "btcusdt@depth@100ms".to_string(),
        snapshot: sample_snapshot(),
        deltas: vec![
            sample_delta(
                1_711_929_699_900,
                98,
                99,
                Some(97),
                vec![[100.0, 1.5]],
                vec![],
            ),
            sample_delta(
                1_711_929_700_050,
                100,
                101,
                Some(99),
                vec![[100.0, 1.0]],
                vec![[101.0, 1.0]],
            ),
            sample_delta(
                1_711_929_700_100,
                102,
                103,
                Some(101),
                vec![[99.0, 0.0]],
                vec![[100.5, 2.0]],
            ),
        ],
    };

    writer
        .write_binance_futures_capture("BTC/USDT", &session)
        .expect("raw depth capture should write");

    let output = import_binance_futures_depth_capture(temp_dir.path(), "BTC/USDT")
        .expect("raw capture should import into normalized dataset");

    let reader = DepthDatasetReader::new(temp_dir.path());
    let (_manifest, rows) = reader
        .read_binance_futures_depth_deltas("BTC/USDT")
        .expect("normalized depth dataset should read");

    assert!(output.data_path.exists());
    assert_eq!(
        rows.len(),
        3,
        "snapshot plus two valid deltas should remain"
    );
    assert_eq!(rows[0].first_update_id, 100);
    assert_eq!(rows[0].final_update_id, 100);
    assert_eq!(rows[0].bids, vec![[100.0, 2.0], [99.0, 3.0]]);
    assert_eq!(rows[1].final_update_id, 101);
    assert_eq!(rows[2].final_update_id, 103);
}
