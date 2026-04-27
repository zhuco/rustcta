use rustcta::backtest::data::exchange_metadata::{ExchangeMetadataReader, ExchangeMetadataWriter};
use rustcta::core::types::{MarketType, TradingPair};
use tempfile::tempdir;

#[test]
fn persists_and_reads_trading_pair_constraints_snapshot() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = ExchangeMetadataWriter::new(temp_dir.path());

    let trading_pair = TradingPair {
        symbol: "BTC/USDT".to_string(),
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        status: "TRADING".to_string(),
        min_order_size: 0.001,
        max_order_size: 1000.0,
        tick_size: 0.1,
        step_size: 0.001,
        min_notional: Some(5.0),
        is_trading: true,
        market_type: MarketType::Futures,
    };

    let output = writer
        .write_trading_pair("binance", "futures", &trading_pair)
        .expect("metadata snapshot should write");

    let reader = ExchangeMetadataReader::new(temp_dir.path());
    let restored = reader
        .read_trading_pair("binance", "futures", "BTC/USDT")
        .expect("metadata snapshot should read");

    assert!(output.exists());
    assert_eq!(restored.symbol, "BTC/USDT");
    assert_eq!(restored.tick_size, 0.1);
    assert_eq!(restored.step_size, 0.001);
    assert_eq!(restored.min_notional, Some(5.0));
    assert_eq!(restored.market_type, MarketType::Futures);
}
