use anyhow::Result;

use super::{BookCache, BookRecorder, SpotSpotTakerArbitrageConfig, SpotVenue};
use crate::data::{BookHealth, WebSocketBookManager, WebSocketBookManagerConfig};
use crate::exchanges::bitget::BitgetSpotClient;
use crate::exchanges::coinex::CoinExSpotClient;
use crate::exchanges::gateio::GateIoSpotClient;
use crate::exchanges::kucoin::KuCoinSpotClient;
use crate::exchanges::mexc::MexcSpotClient;

#[derive(Clone)]
pub struct WebsocketMarketDataRuntime {
    pub recorder: Option<BookRecorder>,
    pub health: BookHealth,
}

pub async fn start_websocket_market_data(
    config: &SpotSpotTakerArbitrageConfig,
    mexc: MexcSpotClient,
    coinex: CoinExSpotClient,
    gateio: GateIoSpotClient,
    bitget: BitgetSpotClient,
    kucoin: KuCoinSpotClient,
    cache: BookCache,
) -> Result<WebsocketMarketDataRuntime> {
    let symbols = if config.websocket.symbols.is_empty() {
        config.symbols.clone()
    } else {
        config.websocket.symbols.clone()
    };
    let recorder = if config.websocket.record_books {
        Some(BookRecorder::start(config.websocket.book_recording_path.clone()).await?)
    } else {
        None
    };
    let shared_recorder = recorder.as_ref().map(|recorder| recorder.shared());
    let health = BookHealth::default();
    let manager = WebSocketBookManager::with_parts(
        cache.shared(),
        health.clone(),
        shared_recorder,
        WebSocketBookManagerConfig {
            symbols,
            stale_book_ms: config.websocket.stale_book_ms,
            reconnect_interval_ms: config.websocket.reconnect_interval_ms,
            heartbeat_timeout_ms: config.websocket.heartbeat_timeout_ms,
            max_reconnect_attempts: config.websocket.max_reconnect_attempts,
            record_books: config.websocket.record_books,
            record_top_of_book_only: config.websocket.record_top_of_book_only,
            book_recording_path: config.websocket.book_recording_path.clone(),
        },
    );
    let exchanges = if config.websocket.exchanges.is_empty() {
        &config.exchanges
    } else {
        &config.websocket.exchanges
    };

    if exchanges
        .iter()
        .any(|exchange| exchange.eq_ignore_ascii_case("mexc"))
    {
        manager.spawn_exchange(mexc);
    }
    if exchanges
        .iter()
        .any(|exchange| exchange.eq_ignore_ascii_case("coinex"))
    {
        manager.spawn_exchange(coinex);
    }
    if exchanges.iter().any(|exchange| {
        matches!(
            exchange.trim().to_ascii_lowercase().as_str(),
            "gate" | "gateio" | "gate.io"
        )
    }) {
        manager.spawn_exchange(gateio);
    }
    if exchanges
        .iter()
        .any(|exchange| exchange.eq_ignore_ascii_case("bitget"))
    {
        manager.spawn_exchange(bitget);
    }
    if exchanges
        .iter()
        .any(|exchange| exchange.eq_ignore_ascii_case("kucoin"))
    {
        manager.spawn_exchange(kucoin);
    }
    Ok(WebsocketMarketDataRuntime { recorder, health })
}

pub async fn mark_reconnect_stale_for_test(
    cache: &BookCache,
    exchange: SpotVenue,
    symbols: &[String],
) {
    cache.mark_exchange_stale(exchange, symbols).await;
}
