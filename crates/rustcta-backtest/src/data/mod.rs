pub mod depth_dataset {
    mod dto {
        pub use crate::schema::DepthDeltaEvent;
    }

    include!("depth_dataset_impl.rs");
}

pub mod depth_capture {
    mod dto {
        pub use crate::schema::DepthDeltaEvent;
    }

    include!("depth_capture_impl.rs");
}

pub mod exchange_metadata {
    mod dto {
        pub use crate::types::BacktestTradingPair as TradingPair;
    }

    include!("exchange_metadata_impl.rs");
}

pub mod kline_dataset {
    mod dto {
        pub use crate::types::BacktestKline as Kline;
    }

    include!("kline_dataset_impl.rs");
}

pub mod trade_dataset {
    mod dto {
        pub use crate::types::BacktestTrade as Trade;
    }

    include!("trade_dataset_impl.rs");
}

pub mod trade_capture {
    mod dto {
        pub use crate::types::{BacktestOrderSide as OrderSide, BacktestTrade as Trade};
    }

    include!("trade_capture_impl.rs");
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use tempfile::tempdir;

    use super::depth_dataset::DepthDatasetWriter;
    use super::exchange_metadata::{ExchangeMetadataReader, ExchangeMetadataWriter};
    use super::kline_dataset::KlineDatasetWriter;
    use super::trade_dataset::TradeDatasetWriter;
    use super::{
        depth_capture::{
            import_binance_futures_depth_capture, DepthCaptureSession, DepthCaptureWriter,
            RawDepthDeltaRecord, RawDepthSnapshot,
        },
        trade_capture::{
            import_binance_futures_trade_capture, RawTradeRecord, TradeCaptureSession,
            TradeCaptureWriter,
        },
    };
    use crate::schema::DepthDeltaEvent;
    use crate::types::{
        BacktestKline, BacktestMarketType, BacktestOrderSide, BacktestTrade, BacktestTradingPair,
    };

    fn sample_depth_delta(
        logical_ts_ms: i64,
        first_update_id: u64,
        final_update_id: u64,
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
            bids: vec![[100.0, 2.0]],
            asks: vec![[101.0, 3.0]],
        }
    }

    fn sample_kline(close_time_ms: i64, close: f64) -> BacktestKline {
        let open_time = Utc.timestamp_millis_opt(close_time_ms - 60_000).unwrap();
        let close_time = Utc.timestamp_millis_opt(close_time_ms).unwrap();
        BacktestKline {
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

    fn sample_trade(id: &str, ts_ms: i64, side: BacktestOrderSide) -> BacktestTrade {
        BacktestTrade {
            id: id.to_string(),
            symbol: "BTC/USDT".to_string(),
            side,
            amount: 0.1,
            price: 100.0,
            timestamp: Utc.timestamp_millis_opt(ts_ms).unwrap(),
            order_id: None,
            fee: None,
        }
    }

    fn sample_trading_pair() -> BacktestTradingPair {
        BacktestTradingPair {
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
            market_type: BacktestMarketType::Futures,
        }
    }

    fn sample_raw_depth_snapshot() -> RawDepthSnapshot {
        RawDepthSnapshot {
            last_update_id: 100,
            timestamp: Utc.timestamp_millis_opt(1_711_929_700_000).unwrap(),
            bids: vec![[100.0, 2.0], [99.0, 3.0]],
            asks: vec![[101.0, 2.0], [102.0, 3.0]],
        }
    }

    fn sample_raw_depth_delta(
        logical_ts_ms: i64,
        first_update_id: u64,
        final_update_id: u64,
        previous_final_update_id: Option<u64>,
    ) -> RawDepthDeltaRecord {
        let logical_ts = Utc.timestamp_millis_opt(logical_ts_ms).unwrap();
        RawDepthDeltaRecord {
            exchange_ts: logical_ts,
            logical_ts,
            first_update_id,
            final_update_id,
            previous_final_update_id,
            bids: vec![[100.0, 1.0]],
            asks: vec![[101.0, 1.0]],
        }
    }

    fn sample_raw_trade(
        id: &str,
        ts_ms: i64,
        side: BacktestOrderSide,
        price: f64,
        quantity: f64,
    ) -> RawTradeRecord {
        let ts = Utc.timestamp_millis_opt(ts_ms).unwrap();
        RawTradeRecord {
            exchange_ts: ts,
            logical_ts: ts,
            trade_id: id.to_string(),
            side,
            price,
            quantity,
        }
    }

    #[test]
    fn depth_dataset_persists_sorted_deduped_rows() {
        let temp_dir = tempdir().expect("temp dir");
        let writer = DepthDatasetWriter::new(temp_dir.path());
        let output = writer
            .write_binance_futures_depth_deltas(
                "BTC/USDT",
                &[
                    sample_depth_delta(1_711_929_780_000, 13, 13),
                    sample_depth_delta(1_711_929_660_000, 11, 11),
                    sample_depth_delta(1_711_929_720_000, 12, 12),
                    sample_depth_delta(1_711_929_720_500, 12, 12),
                ],
            )
            .expect("depth dataset write should succeed");

        assert!(output
            .data_path
            .to_string_lossy()
            .contains("symbol=BTC_USDT"));
        let rows: Vec<DepthDeltaEvent> =
            serde_json::from_slice(&std::fs::read(output.data_path).expect("data should exist"))
                .expect("depth rows should deserialize");
        let update_ids: Vec<u64> = rows.iter().map(|row| row.final_update_id).collect();
        assert_eq!(update_ids, vec![11, 12, 13]);

        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.manifest_path).expect("manifest exists"))
                .expect("manifest should deserialize");
        assert_eq!(manifest["row_count"], 3);
    }

    #[test]
    fn kline_dataset_persists_sorted_deduped_rows() {
        let temp_dir = tempdir().expect("temp dir");
        let writer = KlineDatasetWriter::new(temp_dir.path());
        let output = writer
            .write_binance_futures_klines(
                "BTC/USDT",
                "1m",
                &[
                    sample_kline(1_711_929_780_000, 101.0),
                    sample_kline(1_711_929_660_000, 99.0),
                    sample_kline(1_711_929_720_000, 100.0),
                    sample_kline(1_711_929_720_000, 100.5),
                ],
            )
            .expect("kline dataset write should succeed");

        assert!(output
            .data_path
            .to_string_lossy()
            .contains("symbol=BTC_USDT"));
        let rows: Vec<BacktestKline> =
            serde_json::from_slice(&std::fs::read(output.data_path).expect("data should exist"))
                .expect("kline rows should deserialize");
        let close_prices: Vec<f64> = rows.iter().map(|row| row.close).collect();
        assert_eq!(close_prices, vec![99.0, 100.0, 101.0]);
    }

    #[test]
    fn trade_dataset_persists_sorted_deduped_rows() {
        let temp_dir = tempdir().expect("temp dir");
        let writer = TradeDatasetWriter::new(temp_dir.path());
        let output = writer
            .write_binance_futures_trades(
                "BTC/USDT",
                &[
                    sample_trade("t2", 1_711_929_720_000, BacktestOrderSide::Sell),
                    sample_trade("t1", 1_711_929_660_000, BacktestOrderSide::Buy),
                    sample_trade("t2", 1_711_929_720_000, BacktestOrderSide::Sell),
                ],
            )
            .expect("trade dataset write should succeed");

        let rows: Vec<BacktestTrade> =
            serde_json::from_slice(&std::fs::read(output.data_path).expect("data should exist"))
                .expect("trade rows should deserialize");
        let ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
        assert_eq!(ids, vec!["t1", "t2"]);

        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.manifest_path).expect("manifest exists"))
                .expect("manifest should deserialize");
        assert_eq!(manifest["row_count"], 2);
    }

    #[test]
    fn exchange_metadata_persists_trading_pair_constraints_snapshot() {
        let temp_dir = tempdir().expect("temp dir");
        let writer = ExchangeMetadataWriter::new(temp_dir.path());
        let trading_pair = sample_trading_pair();

        let output = writer
            .write_trading_pair("binance", "futures", &trading_pair)
            .expect("metadata snapshot should write");

        assert!(output.exists());
        assert!(output
            .to_string_lossy()
            .contains("symbol=BTC_USDT/trading_pair.json"));

        let reader = ExchangeMetadataReader::new(temp_dir.path());
        assert!(reader.has_trading_pair("binance", "futures", "BTC/USDT"));

        let restored = reader
            .read_trading_pair("binance", "futures", "BTC/USDT")
            .expect("metadata snapshot should read");
        assert_eq!(restored.symbol, "BTC/USDT");
        assert_eq!(restored.tick_size, 0.1);
        assert_eq!(restored.step_size, 0.001);
        assert_eq!(restored.min_notional, Some(5.0));
        assert_eq!(restored.market_type, BacktestMarketType::Futures);
    }

    #[test]
    fn depth_capture_imports_raw_session_inside_backtest_crate() {
        let temp_dir = tempdir().expect("temp dir");
        let writer = DepthCaptureWriter::new(temp_dir.path());
        let session = DepthCaptureSession {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            stream: "btcusdt@depth@100ms".to_string(),
            snapshot: sample_raw_depth_snapshot(),
            deltas: vec![
                sample_raw_depth_delta(1_711_929_699_900, 98, 99, Some(97)),
                sample_raw_depth_delta(1_711_929_700_050, 100, 101, Some(99)),
                sample_raw_depth_delta(1_711_929_700_100, 102, 103, Some(101)),
            ],
        };

        writer
            .write_binance_futures_capture("BTC/USDT", &session)
            .expect("raw depth capture should write");
        let output = import_binance_futures_depth_capture(temp_dir.path(), "BTC/USDT")
            .expect("raw depth capture should import");
        let rows: Vec<DepthDeltaEvent> =
            serde_json::from_slice(&std::fs::read(output.data_path).expect("data should exist"))
                .expect("depth capture rows should deserialize");

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].first_update_id, 100);
        assert_eq!(rows[2].final_update_id, 103);
    }

    #[test]
    fn trade_capture_imports_raw_session_inside_backtest_crate() {
        let temp_dir = tempdir().expect("temp dir");
        let writer = TradeCaptureWriter::new(temp_dir.path());
        let session = TradeCaptureSession {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            stream: "btcusdt@trade".to_string(),
            trades: vec![
                sample_raw_trade("2", 1_711_929_700_100, BacktestOrderSide::Sell, 100.0, 0.2),
                sample_raw_trade("1", 1_711_929_700_000, BacktestOrderSide::Buy, 99.5, 0.1),
                sample_raw_trade("2", 1_711_929_700_100, BacktestOrderSide::Sell, 100.0, 0.2),
            ],
        };

        writer
            .write_binance_futures_capture("BTC/USDT", &session)
            .expect("raw trade capture should write");
        let output = import_binance_futures_trade_capture(temp_dir.path(), "BTC/USDT")
            .expect("raw trade capture should import");
        let rows: Vec<BacktestTrade> =
            serde_json::from_slice(&std::fs::read(output.data_path).expect("data should exist"))
                .expect("trade capture rows should deserialize");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "1");
        assert_eq!(rows[0].side, BacktestOrderSide::Buy);
        assert_eq!(rows[1].id, "2");
        assert_eq!(rows[1].side, BacktestOrderSide::Sell);
    }
}
