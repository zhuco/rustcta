#![allow(clippy::all)]
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rustcta_smart_money::*;

fn dec(value: i64, scale: u32) -> Decimal {
    Decimal::new(value, scale)
}

fn snapshot(last_update_id: u64) -> BinanceDepthSnapshot {
    BinanceDepthSnapshot {
        last_update_id,
        bids: vec![
            BinanceBookLevel(dec(10000, 0), dec(5, 1)),
            BinanceBookLevel(dec(9990, 0), dec(2, 0)),
        ],
        asks: vec![
            BinanceBookLevel(dec(10010, 0), dec(4, 1)),
            BinanceBookLevel(dec(10020, 0), dec(3, 0)),
        ],
    }
}

fn update(first: u64, final_id: u64, previous: u64) -> BinanceDepthUpdate {
    BinanceDepthUpdate {
        symbol: Some("BTCUSDT".to_string()),
        event_time_millis: Some(1_700_000_001_000),
        first_update_id: first,
        final_update_id: final_id,
        previous_final_update_id: previous,
        bids: vec![BinanceBookLevel(dec(10000, 0), Decimal::ZERO)],
        asks: vec![BinanceBookLevel(dec(10005, 0), dec(1, 0))],
    }
}

#[test]
fn binance_replay_should_bridge_snapshot_and_convert_to_execution_book() {
    let ts = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let mut book = BinanceOrderBookReplay::from_snapshot("BTCUSDT", snapshot(100), ts).unwrap();

    let outcome = book.apply_update(update(99, 101, 98)).unwrap();

    assert_eq!(outcome, ReplayUpdateOutcome::Applied);
    assert_eq!(book.last_update_id(), 101);
    assert_eq!(book.best_bid(), Some(dec(9990, 0)));
    assert_eq!(book.best_ask(), Some(dec(10005, 0)));
    assert_eq!(book.mid_price(), Some(dec(19995, 0) / Decimal::new(2, 0)));

    let execution_book = book.to_order_book_snapshot();
    assert_eq!(execution_book.symbol, "BTCUSDT");
    assert_eq!(execution_book.best_bid(), Some(dec(9990, 0)));
    assert_eq!(execution_book.best_ask(), Some(dec(10005, 0)));
    assert_eq!(execution_book.bids[0].quantity, dec(2, 0));
    assert_eq!(execution_book.asks[0].quantity, dec(1, 0));
}

#[test]
fn binance_replay_should_reject_gap_after_first_applied_update() {
    let ts = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let mut book = BinanceOrderBookReplay::from_snapshot("BTCUSDT", snapshot(100), ts).unwrap();
    book.apply_update(update(101, 101, 100)).unwrap();

    let err = book.apply_update(update(103, 103, 101)).unwrap_err();

    assert_eq!(
        err,
        BinanceReplayError::UpdateGap {
            expected_first_update_id: 102,
            expected_previous_final_update_id: 101,
            first_update_id: 103,
            previous_final_update_id: 101,
        }
    );
}

#[test]
fn binance_replay_should_ignore_stale_updates() {
    let ts = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let mut book = BinanceOrderBookReplay::from_snapshot("BTCUSDT", snapshot(100), ts).unwrap();

    let outcome = book.apply_update(update(99, 100, 98)).unwrap();

    assert_eq!(outcome, ReplayUpdateOutcome::Stale);
    assert_eq!(book.best_bid(), Some(dec(10000, 0)));
    assert_eq!(book.best_ask(), Some(dec(10010, 0)));
}

#[test]
fn binance_replay_should_deserialize_binance_level_tuples() {
    let update: BinanceDepthUpdate = serde_json::from_str(
        r#"{
            "s": "BTCUSDT",
            "E": 1700000001000,
            "U": 101,
            "u": 101,
            "pu": 100,
            "b": [["10000.50", "0.25"]],
            "a": [["10001.00", "1.50"]]
        }"#,
    )
    .unwrap();

    assert_eq!(
        update.bids[0],
        BinanceBookLevel(dec(1000050, 2), dec(25, 2))
    );
    assert_eq!(
        update.asks[0],
        BinanceBookLevel(dec(1000100, 2), dec(150, 2))
    );
}
