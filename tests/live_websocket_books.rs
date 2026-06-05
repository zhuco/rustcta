use std::time::Duration;

use rustcta::exchanges::coinex::{CoinExSpotClient, CoinExSpotConfig};
use rustcta::exchanges::mexc::{MexcSpotClient, MexcSpotConfig};
use rustcta::exchanges::unified::ExchangeClient;

fn live_ws_enabled() -> bool {
    std::env::var("ENABLE_LIVE_WS_TESTS")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[tokio::test]
#[ignore]
async fn live_mexc_public_ws_should_receive_book_event() {
    if !live_ws_enabled() {
        eprintln!("set ENABLE_LIVE_WS_TESTS=true to run live public websocket test");
        return;
    }
    let client = MexcSpotClient::new(MexcSpotConfig {
        dry_run: true,
        enable_private_stream: false,
        enabled_symbols: vec!["BTCUSDT".to_string()],
        ..MexcSpotConfig::default()
    });
    let mut rx = client
        .subscribe_orderbook(vec!["BTCUSDT".to_string()])
        .await
        .unwrap();
    let book = tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(book.exchange, "mexc");
    assert_eq!(book.symbol, "BTCUSDT");
    assert!(book.best_bid.is_some());
    assert!(book.best_ask.is_some());
}

#[tokio::test]
#[ignore]
async fn live_coinex_public_ws_should_receive_book_event() {
    if !live_ws_enabled() {
        eprintln!("set ENABLE_LIVE_WS_TESTS=true to run live public websocket test");
        return;
    }
    let client = CoinExSpotClient::new(CoinExSpotConfig {
        dry_run: true,
        enable_private_stream: false,
        enabled_symbols: vec!["BTCUSDT".to_string()],
        ..CoinExSpotConfig::default()
    });
    let mut rx = client
        .subscribe_orderbook(vec!["BTCUSDT".to_string()])
        .await
        .unwrap();
    let book = tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(book.exchange, "coinex");
    assert_eq!(book.symbol, "BTCUSDT");
    assert!(book.best_bid.is_some());
    assert!(book.best_ask.is_some());
}
