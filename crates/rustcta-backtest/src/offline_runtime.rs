use std::path::PathBuf;

use anyhow::Result;
use chrono::{DateTime, Utc};

use self::deps::{
    BacktestEvent, BacktestKline, DepthDatasetReader, DepthReplay, KlineReplay, ReplayPartition,
    TradeDatasetReader, TradeReplay,
};

#[derive(Debug, Clone)]
pub struct BacktestRuntimeConfig {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub interval: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub output: PathBuf,
}

#[derive(Debug, Clone)]
pub struct BacktestRuntime {
    config: BacktestRuntimeConfig,
}

impl BacktestRuntime {
    pub fn new(config: BacktestRuntimeConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &BacktestRuntimeConfig {
        &self.config
    }

    pub fn load_kline_replay(&self) -> Result<KlineReplay> {
        self.load_kline_replay_for_interval(&self.config.interval)
    }

    pub fn load_kline_replay_for_interval(&self, interval: &str) -> Result<KlineReplay> {
        KlineReplay::load_binance_futures(
            &self.config.output,
            &self.config.symbol,
            interval,
            Some(self.config.start),
            Some(self.config.end),
        )
    }

    pub fn collect_kline_candles_for_interval(&self, interval: &str) -> Result<Vec<BacktestKline>> {
        Ok(self
            .load_kline_replay_for_interval(interval)?
            .collect_events()
            .into_iter()
            .filter_map(|event| match event {
                BacktestEvent::Kline(kline) => Some(kline.kline),
                _ => None,
            })
            .collect::<Vec<_>>())
    }

    pub fn load_depth_replay(&self) -> Result<Option<DepthReplay>> {
        let reader = DepthDatasetReader::new(&self.config.output);
        if !reader.has_binance_futures_depth_deltas(&self.config.symbol) {
            return Ok(None);
        }

        Ok(Some(DepthReplay::load_binance_futures(
            &self.config.output,
            &self.config.symbol,
            Some(self.config.start),
            Some(self.config.end),
        )?))
    }

    pub fn load_trade_replay(&self) -> Result<Option<TradeReplay>> {
        let reader = TradeDatasetReader::new(&self.config.output);
        if !reader.has_binance_futures_trades(&self.config.symbol) {
            return Ok(None);
        }

        Ok(Some(TradeReplay::load_binance_futures(
            &self.config.output,
            &self.config.symbol,
            Some(self.config.start),
            Some(self.config.end),
        )?))
    }

    pub fn plan_kline_partitions(&self, partitions: usize) -> Result<Vec<ReplayPartition>> {
        let replay = self.load_kline_replay()?;
        Ok(replay.plan_partitions(partitions))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use tempfile::tempdir;

    use super::{BacktestRuntime, BacktestRuntimeConfig};
    use super::deps::{
        BacktestKline, BacktestOrderSide, BacktestTrade, DepthDatasetWriter, DepthDeltaEvent,
        KlineDatasetWriter, TradeDatasetWriter,
    };

    fn ts(ms: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_millis_opt(ms).unwrap()
    }

    fn sample_kline(close_time_ms: i64, close: f64) -> BacktestKline {
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

    fn sample_depth_delta(ts_ms: i64, update_id: u64) -> DepthDeltaEvent {
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

    fn sample_trade(ts_ms: i64, id: &str) -> BacktestTrade {
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

    fn runtime(root: std::path::PathBuf) -> BacktestRuntime {
        BacktestRuntime::new(BacktestRuntimeConfig {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            interval: "1m".to_string(),
            start: ts(1_000),
            end: ts(6_000),
            output: root,
        })
    }

    #[test]
    fn offline_runtime_loads_replays_and_partitions_root_free() {
        let temp_dir = tempdir().expect("temp dir");
        KlineDatasetWriter::new(temp_dir.path())
            .write_binance_futures_klines(
                "BTC/USDT",
                "1m",
                &[
                    sample_kline(2_000, 99.0),
                    sample_kline(4_000, 100.0),
                    sample_kline(6_000, 101.0),
                ],
            )
            .expect("kline dataset should write");
        DepthDatasetWriter::new(temp_dir.path())
            .write_binance_futures_depth_deltas(
                "BTC/USDT",
                &[sample_depth_delta(2_500, 1), sample_depth_delta(4_500, 2)],
            )
            .expect("depth dataset should write");
        TradeDatasetWriter::new(temp_dir.path())
            .write_binance_futures_trades(
                "BTC/USDT",
                &[sample_trade(3_000, "t1"), sample_trade(5_000, "t2")],
            )
            .expect("trade dataset should write");

        let runtime = runtime(temp_dir.path().to_path_buf());
        let kline_events = runtime
            .load_kline_replay()
            .expect("kline replay should load")
            .collect_events();
        let depth_events = runtime
            .load_depth_replay()
            .expect("depth replay should load")
            .expect("depth replay should exist")
            .collect_events();
        let trade_events = runtime
            .load_trade_replay()
            .expect("trade replay should load")
            .expect("trade replay should exist")
            .collect_events();
        let partitions = runtime
            .plan_kline_partitions(2)
            .expect("partitions should plan");

        assert_eq!(kline_events.len(), 3);
        assert_eq!(depth_events.len(), 2);
        assert_eq!(trade_events.len(), 2);
        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn offline_runtime_returns_none_for_missing_optional_streams() {
        let temp_dir = tempdir().expect("temp dir");
        KlineDatasetWriter::new(temp_dir.path())
            .write_binance_futures_klines("BTC/USDT", "1m", &[sample_kline(2_000, 99.0)])
            .expect("kline dataset should write");

        let runtime = runtime(temp_dir.path().to_path_buf());
        assert!(runtime
            .load_depth_replay()
            .expect("depth check should succeed")
            .is_none());
        assert!(runtime
            .load_trade_replay()
            .expect("trade check should succeed")
            .is_none());
    }
}
