use chrono::{TimeZone, Utc};
use rustcta::backtest::data::kline_dataset::KlineDatasetWriter;
use rustcta::core::types::Interval;
use rustcta::core::types::Kline;
use rustcta::exchanges::binance::build_binance_kline_query_params;
use tempfile::tempdir;

fn sample_kline(close_time_ms: i64, close: f64) -> Kline {
    let open_time = Utc.timestamp_millis_opt(close_time_ms - 60_000).unwrap();
    let close_time = Utc.timestamp_millis_opt(close_time_ms).unwrap();

    Kline {
        symbol: "BTC/USDT".to_string(),
        interval: "1m".to_string(),
        open_time,
        close_time,
        open: close - 1.0,
        high: close + 1.0,
        low: close - 2.0,
        close,
        volume: 10.0,
        quote_volume: 1000.0,
        trade_count: 42,
    }
}

#[test]
fn persists_manifest_and_chronological_deduped_klines() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());

    let klines = vec![
        sample_kline(1_711_929_780_000, 101.0),
        sample_kline(1_711_929_660_000, 99.0),
        sample_kline(1_711_929_720_000, 100.0),
        sample_kline(1_711_929_720_000, 100.5),
    ];

    let output = writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("dataset write should succeed");

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
    let rows: Vec<Kline> = serde_json::from_str(&persisted).expect("parse persisted klines");
    assert_eq!(rows.len(), 3, "duplicate close times should be collapsed");

    let close_prices: Vec<f64> = rows.iter().map(|row| row.close).collect();
    assert_eq!(close_prices, vec![99.0, 100.0, 101.0]);

    let close_times: Vec<_> = rows.iter().map(|row| row.close_time).collect();
    assert!(close_times.windows(2).all(|pair| pair[0] < pair[1]));

    let manifest = std::fs::read_to_string(&output.manifest_path).expect("read manifest");
    let manifest_json: serde_json::Value =
        serde_json::from_str(&manifest).expect("parse manifest json");

    assert_eq!(manifest_json["exchange"], "binance");
    assert_eq!(manifest_json["market"], "futures");
    assert_eq!(manifest_json["symbol"], "BTC/USDT");
    assert_eq!(manifest_json["interval"], "1m");
    assert_eq!(manifest_json["row_count"], 3);
}

#[test]
fn backtest_kline_query_includes_window_parameters() {
    let start = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let end = Utc.timestamp_millis_opt(1_711_933_200_000).unwrap();

    let params = build_binance_kline_query_params(
        "BTCUSDT",
        Interval::OneMinute,
        Some(500),
        Some(start),
        Some(end),
    );

    assert_eq!(params.get("symbol").map(String::as_str), Some("BTCUSDT"));
    assert_eq!(params.get("interval").map(String::as_str), Some("1m"));
    assert_eq!(params.get("limit").map(String::as_str), Some("500"));
    assert_eq!(
        params.get("startTime").map(String::as_str),
        Some("1711929600000")
    );
    assert_eq!(
        params.get("endTime").map(String::as_str),
        Some("1711933200000")
    );
}
