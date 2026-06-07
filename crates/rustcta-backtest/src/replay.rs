include!("replay_impl.rs");

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::schema::DepthDeltaEvent;
    use crate::types::{BacktestKline, BacktestOrderSide, BacktestTrade};

    fn temp_root(test_name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .expect("timestamp nanos should fit");
        std::env::temp_dir().join(format!(
            "rustcta-backtest-{test_name}-{}-{suffix}",
            std::process::id()
        ))
    }

    fn write_json(path: &Path, value: impl serde::Serialize) {
        fs::create_dir_all(path.parent().expect("path should have parent"))
            .expect("create dataset dir");
        fs::write(
            path,
            serde_json::to_vec_pretty(&value).expect("serialize test data"),
        )
        .expect("write test data");
    }

    fn ts(ms: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_millis_opt(ms).unwrap()
    }

    fn kline(close_time_ms: i64, close: f64) -> BacktestKline {
        BacktestKline {
            symbol: "BTC/USDT".to_string(),
            interval: "1m".to_string(),
            open_time: ts(close_time_ms - 60_000),
            close_time: ts(close_time_ms),
            open: close - 1.0,
            high: close + 1.0,
            low: close - 2.0,
            close,
            volume: 10.0,
            quote_volume: 1000.0,
            trade_count: 42,
        }
    }

    fn trade(ts_ms: i64, id: &str) -> BacktestTrade {
        BacktestTrade {
            id: id.to_string(),
            symbol: "BTC/USDT".to_string(),
            side: BacktestOrderSide::Buy,
            amount: 1.0,
            price: 100.0,
            timestamp: ts(ts_ms),
            order_id: None,
            fee: None,
        }
    }

    fn depth_delta(ts_ms: i64, update_id: u64) -> DepthDeltaEvent {
        DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts(ts_ms),
            logical_ts: ts(ts_ms),
            first_update_id: update_id,
            final_update_id: update_id,
            bids: vec![[99.0, 1.0]],
            asks: vec![[101.0, 1.0]],
        }
    }

    #[test]
    fn loads_file_readers_and_merges_streams_without_legacy_root() {
        let root = temp_root("replay");
        let symbol_dir = root
            .join("normalized")
            .join("exchange=binance")
            .join("market=futures")
            .join("symbol=BTC_USDT");

        write_json(
            &symbol_dir.join("interval=1m").join("manifest.json"),
            serde_json::json!({
                "exchange": "binance",
                "market": "futures",
                "symbol": "BTC/USDT",
                "interval": "1m",
                "row_count": 2,
                "first_close_time": ts(2_000),
                "last_close_time": ts(4_000),
            }),
        );
        write_json(
            &symbol_dir.join("interval=1m").join("klines.json"),
            vec![kline(4_000, 101.0), kline(2_000, 99.0)],
        );

        write_json(
            &symbol_dir.join("stream=depth_delta").join("manifest.json"),
            serde_json::json!({
                "exchange": "binance",
                "market": "futures",
                "symbol": "BTC/USDT",
                "row_count": 1,
                "first_logical_ts": ts(1_000),
                "last_logical_ts": ts(1_000),
            }),
        );
        write_json(
            &symbol_dir
                .join("stream=depth_delta")
                .join("depth_deltas.json"),
            vec![depth_delta(1_000, 1)],
        );

        write_json(
            &symbol_dir.join("stream=trade").join("manifest.json"),
            serde_json::json!({
                "exchange": "binance",
                "market": "futures",
                "symbol": "BTC/USDT",
                "row_count": 1,
                "first_trade_ts": ts(2_000),
                "last_trade_ts": ts(2_000),
            }),
        );
        write_json(
            &symbol_dir.join("stream=trade").join("trades.json"),
            vec![trade(2_000, "t1")],
        );

        let kline_replay = KlineReplay::load_binance_futures(&root, "BTC/USDT", "1m", None, None)
            .expect("load kline replay");
        let depth_replay = DepthReplay::load_binance_futures(&root, "BTC/USDT", None, None)
            .expect("load depth replay");
        let trade_replay = TradeReplay::load_binance_futures(&root, "BTC/USDT", None, None)
            .expect("load trade replay");

        let merged = MergedReplay::from_streams(vec![
            kline_replay.collect_events(),
            depth_replay.collect_events(),
            trade_replay.collect_events(),
        ]);
        let kinds = merged
            .collect_events()
            .into_iter()
            .map(|event| match event {
                BacktestEvent::DepthDelta(_) => "depth",
                BacktestEvent::Trade(_) => "trade",
                BacktestEvent::Kline(_) => "kline",
                _ => "other",
            })
            .collect::<Vec<_>>();

        assert_eq!(kinds, vec!["depth", "trade", "kline", "kline"]);
        fs::remove_dir_all(root).expect("remove temp replay root");
    }
}
