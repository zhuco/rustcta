use chrono::{TimeZone, Utc};
use rustcta::backtest::data::trade_dataset::TradeDatasetWriter;
use rustcta::core::types::{OrderSide, Trade};
use tempfile::tempdir;

fn sample_trade(id: &str, ts_ms: i64, side: OrderSide, price: f64, amount: f64) -> Trade {
    Trade {
        id: id.to_string(),
        symbol: "BTC/USDT".to_string(),
        side,
        amount,
        price,
        timestamp: Utc.timestamp_millis_opt(ts_ms).unwrap(),
        order_id: None,
        fee: None,
    }
}

#[test]
fn persists_manifest_and_chronological_deduped_trades() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = TradeDatasetWriter::new(temp_dir.path());

    let trades = vec![
        sample_trade("t2", 1_711_929_720_000, OrderSide::Sell, 100.0, 0.2),
        sample_trade("t1", 1_711_929_660_000, OrderSide::Buy, 99.0, 0.1),
        sample_trade("t2", 1_711_929_720_000, OrderSide::Sell, 100.0, 0.2),
    ];

    let output = writer
        .write_binance_futures_trades("BTC/USDT", &trades)
        .expect("trade dataset write should succeed");

    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&output.manifest_path).expect("manifest should exist"),
    )
    .expect("manifest should deserialize");
    let persisted: Vec<Trade> =
        serde_json::from_slice(&std::fs::read(&output.data_path).expect("data should exist"))
            .expect("trade dataset should deserialize");

    assert_eq!(manifest["exchange"], "binance");
    assert_eq!(manifest["market"], "futures");
    assert_eq!(manifest["symbol"], "BTC/USDT");
    assert_eq!(manifest["row_count"], 2);
    assert_eq!(persisted.len(), 2);
    assert_eq!(persisted[0].id, "t1");
    assert_eq!(persisted[1].id, "t2");
    assert!(persisted[0].timestamp < persisted[1].timestamp);
}
