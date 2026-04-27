use chrono::{TimeZone, Utc};
use rustcta::backtest::data::trade_capture::{
    import_binance_futures_trade_capture, RawTradeRecord, TradeCaptureSession, TradeCaptureWriter,
};
use rustcta::backtest::data::trade_dataset::TradeDatasetReader;
use rustcta::core::types::OrderSide;
use tempfile::tempdir;

fn sample_trade(id: &str, ts_ms: i64, side: OrderSide, price: f64, amount: f64) -> RawTradeRecord {
    let ts = Utc.timestamp_millis_opt(ts_ms).unwrap();
    RawTradeRecord {
        exchange_ts: ts,
        logical_ts: ts,
        trade_id: id.to_string(),
        side,
        price,
        quantity: amount,
    }
}

#[test]
fn imports_raw_capture_into_normalized_trade_dataset() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = TradeCaptureWriter::new(temp_dir.path());

    let session = TradeCaptureSession {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        stream: "btcusdt@trade".to_string(),
        trades: vec![
            sample_trade("2", 1_711_929_700_100, OrderSide::Sell, 100.0, 0.2),
            sample_trade("1", 1_711_929_700_000, OrderSide::Buy, 99.5, 0.1),
            sample_trade("2", 1_711_929_700_100, OrderSide::Sell, 100.0, 0.2),
        ],
    };

    writer
        .write_binance_futures_capture("BTC/USDT", &session)
        .expect("raw trade capture should write");

    let output = import_binance_futures_trade_capture(temp_dir.path(), "BTC/USDT")
        .expect("raw capture should import into normalized trade dataset");

    let reader = TradeDatasetReader::new(temp_dir.path());
    let (_manifest, rows) = reader
        .read_binance_futures_trades("BTC/USDT")
        .expect("normalized trade dataset should read");

    assert!(output.data_path.exists());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, "1");
    assert_eq!(rows[1].id, "2");
    assert_eq!(rows[0].side, OrderSide::Buy);
    assert_eq!(rows[1].side, OrderSide::Sell);
    assert!(rows[0].timestamp < rows[1].timestamp);
}
