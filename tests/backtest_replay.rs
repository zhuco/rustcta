use chrono::{TimeZone, Utc};
use rustcta::backtest::data::depth_dataset::DepthDatasetWriter;
use rustcta::backtest::data::kline_dataset::KlineDatasetWriter;
use rustcta::backtest::data::trade_dataset::TradeDatasetWriter;
use rustcta::backtest::replay::{DepthReplay, KlineReplay, MergedReplay, TradeReplay};
use rustcta::backtest::schema::{BacktestEvent, DepthDeltaEvent};
use rustcta::core::types::{Kline, OrderSide, Trade};
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

fn sample_depth_delta(
    logical_ts_ms: i64,
    update_id: u64,
    bid_price: f64,
    ask_price: f64,
) -> DepthDeltaEvent {
    let logical_ts = Utc.timestamp_millis_opt(logical_ts_ms).unwrap();
    DepthDeltaEvent {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        exchange_ts: logical_ts,
        logical_ts,
        first_update_id: update_id,
        final_update_id: update_id,
        bids: vec![[bid_price, 2.0]],
        asks: vec![[ask_price, 3.0]],
    }
}

fn sample_trade(ts_ms: i64, id: &str, side: OrderSide, price: f64, amount: f64) -> Trade {
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
fn replays_persisted_kline_dataset_in_chronological_order() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());

    let klines = vec![
        sample_kline(1_711_929_780_000, 101.0),
        sample_kline(1_711_929_660_000, 99.0),
        sample_kline(1_711_929_720_000, 100.0),
    ];
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("dataset write should succeed");

    let replay = KlineReplay::load_binance_futures(temp_dir.path(), "BTC/USDT", "1m", None, None)
        .expect("replay should load");

    let events = replay.collect_events();
    assert_eq!(events.len(), 3);

    let close_prices: Vec<f64> = events
        .iter()
        .map(|event| match event {
            BacktestEvent::Kline(kline_event) => kline_event.kline.close,
            _ => panic!("expected only kline events in kline replay"),
        })
        .collect();
    assert_eq!(close_prices, vec![99.0, 100.0, 101.0]);

    let logical_times: Vec<_> = events.iter().map(BacktestEvent::logical_ts).collect();
    assert!(logical_times.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn replay_filters_events_by_time_window() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());

    let klines = vec![
        sample_kline(1_711_929_660_000, 99.0),
        sample_kline(1_711_929_720_000, 100.0),
        sample_kline(1_711_929_780_000, 101.0),
    ];
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("dataset write should succeed");

    let start = Some(Utc.timestamp_millis_opt(1_711_929_700_000).unwrap());
    let end = Some(Utc.timestamp_millis_opt(1_711_929_760_000).unwrap());
    let replay = KlineReplay::load_binance_futures(temp_dir.path(), "BTC/USDT", "1m", start, end)
        .expect("replay should load");

    let events = replay.collect_events();
    assert_eq!(events.len(), 1);

    match &events[0] {
        BacktestEvent::Kline(kline_event) => {
            assert_eq!(kline_event.kline.close, 100.0);
            assert_eq!(kline_event.symbol, "BTC/USDT");
            assert_eq!(kline_event.interval, "1m");
        }
        _ => panic!("expected only kline events in filtered kline replay"),
    }
}

#[test]
fn replay_builds_deterministic_partitions_for_parallel_execution() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());

    let klines = vec![
        sample_kline(1_711_929_660_000, 99.0),
        sample_kline(1_711_929_720_000, 100.0),
        sample_kline(1_711_929_780_000, 101.0),
        sample_kline(1_711_929_840_000, 102.0),
        sample_kline(1_711_929_900_000, 103.0),
    ];
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("dataset write should succeed");

    let replay = KlineReplay::load_binance_futures(temp_dir.path(), "BTC/USDT", "1m", None, None)
        .expect("replay should load");

    let partitions = replay.plan_partitions(2);
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].start_index, 0);
    assert_eq!(partitions[0].end_index, 3);
    assert_eq!(partitions[1].start_index, 3);
    assert_eq!(partitions[1].end_index, 5);
    assert!(partitions[0].start_ts < partitions[0].end_ts);
    assert!(partitions[1].start_ts < partitions[1].end_ts);
}

#[test]
fn merged_replay_loads_depth_and_kline_streams_in_order() {
    let temp_dir = tempdir().expect("temp dir");
    let kline_writer = KlineDatasetWriter::new(temp_dir.path());
    let depth_writer = DepthDatasetWriter::new(temp_dir.path());

    let klines = vec![
        sample_kline(1_711_929_720_000, 100.0),
        sample_kline(1_711_929_780_000, 101.0),
    ];
    let depth_deltas = vec![
        sample_depth_delta(1_711_929_719_999, 11, 99.0, 101.0),
        sample_depth_delta(1_711_929_720_001, 12, 99.5, 100.0),
    ];

    kline_writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("kline dataset write should succeed");
    depth_writer
        .write_binance_futures_depth_deltas("BTC/USDT", &depth_deltas)
        .expect("depth dataset write should succeed");

    let kline_replay =
        KlineReplay::load_binance_futures(temp_dir.path(), "BTC/USDT", "1m", None, None)
            .expect("kline replay should load");
    let depth_replay = DepthReplay::load_binance_futures(temp_dir.path(), "BTC/USDT", None, None)
        .expect("depth replay should load");

    let merged = MergedReplay::from_streams(vec![
        kline_replay.collect_events(),
        depth_replay.collect_events(),
    ]);
    let kinds = merged
        .collect_events()
        .into_iter()
        .map(|event| match event {
            BacktestEvent::DepthDelta(_) => "depth",
            BacktestEvent::Kline(_) => "kline",
            _ => "other",
        })
        .collect::<Vec<_>>();

    assert_eq!(kinds, vec!["depth", "kline", "depth", "kline"]);
}

#[test]
fn merged_replay_loads_trade_depth_and_kline_streams_in_order() {
    let temp_dir = tempdir().expect("temp dir");
    let kline_writer = KlineDatasetWriter::new(temp_dir.path());
    let depth_writer = DepthDatasetWriter::new(temp_dir.path());
    let trade_writer = TradeDatasetWriter::new(temp_dir.path());

    let klines = vec![
        sample_kline(1_711_929_720_000, 100.0),
        sample_kline(1_711_929_780_000, 101.0),
    ];
    let depth_deltas = vec![
        sample_depth_delta(1_711_929_719_999, 11, 99.0, 101.0),
        sample_depth_delta(1_711_929_720_001, 12, 99.5, 100.0),
    ];
    let trades = vec![
        sample_trade(1_711_929_720_000, "t1", OrderSide::Sell, 100.0, 0.3),
        sample_trade(1_711_929_780_000, "t2", OrderSide::Buy, 101.0, 0.4),
    ];

    kline_writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("kline dataset write should succeed");
    depth_writer
        .write_binance_futures_depth_deltas("BTC/USDT", &depth_deltas)
        .expect("depth dataset write should succeed");
    trade_writer
        .write_binance_futures_trades("BTC/USDT", &trades)
        .expect("trade dataset write should succeed");

    let kline_replay =
        KlineReplay::load_binance_futures(temp_dir.path(), "BTC/USDT", "1m", None, None)
            .expect("kline replay should load");
    let depth_replay = DepthReplay::load_binance_futures(temp_dir.path(), "BTC/USDT", None, None)
        .expect("depth replay should load");
    let trade_replay = TradeReplay::load_binance_futures(temp_dir.path(), "BTC/USDT", None, None)
        .expect("trade replay should load");

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

    assert_eq!(
        kinds,
        vec!["depth", "trade", "kline", "depth", "trade", "kline"]
    );
}
