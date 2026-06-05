use anyhow::Result;
use tokio::time::{sleep, Duration};

use crate::data::{
    BookCache, BookEvent, BookEventKind, BookHealth, BookRecorder, BookRecorderConfig, BookSource,
};
use crate::exchanges::unified::{ExchangeClient, MarketType, OrderBookSnapshot};

#[derive(Debug, Clone)]
pub struct WebSocketBookManagerConfig {
    pub symbols: Vec<String>,
    pub stale_book_ms: u64,
    pub reconnect_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub max_reconnect_attempts: u32,
    pub record_books: bool,
    pub record_top_of_book_only: bool,
    pub book_recording_path: String,
}

impl Default for WebSocketBookManagerConfig {
    fn default() -> Self {
        Self {
            symbols: Vec::new(),
            stale_book_ms: 1_000,
            reconnect_interval_ms: 1_000,
            heartbeat_timeout_ms: 10_000,
            max_reconnect_attempts: 0,
            record_books: false,
            record_top_of_book_only: true,
            book_recording_path: "data/book_events.jsonl".to_string(),
        }
    }
}

#[derive(Clone)]
pub struct WebSocketBookManager {
    cache: BookCache,
    health: BookHealth,
    recorder: Option<BookRecorder>,
    config: WebSocketBookManagerConfig,
}

impl WebSocketBookManager {
    pub async fn new(config: WebSocketBookManagerConfig) -> Result<Self> {
        let recorder = if config.record_books {
            Some(
                BookRecorder::start(BookRecorderConfig {
                    path: config.book_recording_path.clone(),
                    top_of_book_only: config.record_top_of_book_only,
                    capacity: 8_192,
                })
                .await?,
            )
        } else {
            None
        };
        Ok(Self {
            cache: BookCache::default(),
            health: BookHealth::default(),
            recorder,
            config,
        })
    }

    pub fn with_parts(
        cache: BookCache,
        health: BookHealth,
        recorder: Option<BookRecorder>,
        config: WebSocketBookManagerConfig,
    ) -> Self {
        Self {
            cache,
            health,
            recorder,
            config,
        }
    }

    pub fn cache(&self) -> BookCache {
        self.cache.clone()
    }

    pub fn health(&self) -> BookHealth {
        self.health.clone()
    }

    pub fn recorder(&self) -> Option<BookRecorder> {
        self.recorder.clone()
    }

    pub fn spawn_exchange<C>(&self, client: C)
    where
        C: ExchangeClient + Clone + Send + Sync + 'static,
    {
        let symbols = self.config.symbols.clone();
        let cache = self.cache.clone();
        let health = self.health.clone();
        let recorder = self.recorder.clone();
        let reconnect_interval_ms = self.config.reconnect_interval_ms;
        let heartbeat_timeout_ms = self.config.heartbeat_timeout_ms;
        let max_reconnect_attempts = self.config.max_reconnect_attempts;
        tokio::spawn(async move {
            run_exchange_loop(
                client,
                symbols,
                cache,
                health,
                recorder,
                reconnect_interval_ms,
                heartbeat_timeout_ms,
                max_reconnect_attempts,
            )
            .await;
        });
    }
}

async fn run_exchange_loop<C>(
    client: C,
    symbols: Vec<String>,
    cache: BookCache,
    health: BookHealth,
    recorder: Option<BookRecorder>,
    reconnect_interval_ms: u64,
    heartbeat_timeout_ms: u64,
    max_reconnect_attempts: u32,
) where
    C: ExchangeClient + Clone + Send + Sync + 'static,
{
    let exchange = client.exchange_name().to_string();
    let market_type = client.market_type();
    let mut attempts = 0_u32;
    loop {
        if max_reconnect_attempts > 0 && attempts >= max_reconnect_attempts {
            health
                .mark_disconnected(&exchange, "max reconnect attempts")
                .await;
            log::warn!(
                "websocket book manager stopping exchange={} after {} attempts",
                exchange,
                attempts
            );
            break;
        }

        log::info!(
            "websocket book manager subscribing exchange={} market={:?} symbols={}",
            exchange,
            market_type,
            symbols.join(",")
        );
        health.mark_connected(&exchange, market_type).await;
        let stream = client.subscribe_orderbook(symbols.clone()).await;
        let mut rx = match stream {
            Ok(rx) => rx,
            Err(error) => {
                attempts += 1;
                health.mark_reconnect(&exchange, market_type).await;
                mark_symbols_stale(
                    &cache,
                    &health,
                    &exchange,
                    market_type,
                    &symbols,
                    "subscribe error",
                )
                .await;
                log::warn!(
                    "websocket book manager subscribe failed exchange={} error={}",
                    exchange,
                    error
                );
                sleep(Duration::from_millis(reconnect_interval_ms)).await;
                continue;
            }
        };

        let mut last_sequence_by_symbol = std::collections::HashMap::<String, u64>::new();
        loop {
            match tokio::time::timeout(Duration::from_millis(heartbeat_timeout_ms), rx.recv()).await
            {
                Err(_) => {
                    attempts += 1;
                    health.mark_heartbeat_timeout(&exchange, market_type).await;
                    health.mark_reconnect(&exchange, market_type).await;
                    mark_symbols_stale(
                        &cache,
                        &health,
                        &exchange,
                        market_type,
                        &symbols,
                        "heartbeat timeout",
                    )
                    .await;
                    log::warn!(
                        "websocket book manager heartbeat timeout exchange={} market={:?}",
                        exchange,
                        market_type
                    );
                    break;
                }
                Ok(None) => {
                    attempts += 1;
                    health.mark_reconnect(&exchange, market_type).await;
                    mark_symbols_stale(
                        &cache,
                        &health,
                        &exchange,
                        market_type,
                        &symbols,
                        "stream ended",
                    )
                    .await;
                    log::warn!(
                        "websocket book manager stream ended exchange={} market={:?}",
                        exchange,
                        market_type
                    );
                    break;
                }
                Ok(Some(snapshot)) => {
                    attempts = 0;
                    if sequence_gap_detected(&mut last_sequence_by_symbol, &snapshot) {
                        health.mark_sequence_gap(&exchange, market_type).await;
                        log::warn!(
                            "websocket book manager non-monotonic sequence exchange={} symbol={} sequence={:?}",
                            exchange,
                            snapshot.symbol,
                            snapshot.sequence
                        );
                    }
                    let event = BookEvent::from_snapshot(
                        snapshot.clone(),
                        BookSource::Websocket,
                        BookEventKind::Snapshot,
                    );
                    cache.update_from_event(event.clone()).await;
                    health
                        .mark_message(
                            &exchange,
                            market_type,
                            &snapshot.symbol,
                            snapshot.latency_ms,
                            snapshot.received_at,
                        )
                        .await;
                    if let Some(recorder) = recorder.as_ref() {
                        recorder.try_record_event(&event);
                        health
                            .set_recorder_dropped_events(&exchange, recorder.dropped_events())
                            .await;
                    }
                }
            }
        }

        sleep(Duration::from_millis(reconnect_interval_ms)).await;
    }
}

async fn mark_symbols_stale(
    cache: &BookCache,
    health: &BookHealth,
    exchange: &str,
    market_type: MarketType,
    symbols: &[String],
    reason: &str,
) {
    cache
        .mark_exchange_stale(exchange, market_type, symbols, reason)
        .await;
    for symbol in symbols {
        health
            .mark_symbol_stale(exchange, market_type, symbol)
            .await;
    }
}

fn sequence_gap_detected(
    last_sequence_by_symbol: &mut std::collections::HashMap<String, u64>,
    snapshot: &OrderBookSnapshot,
) -> bool {
    let Some(sequence) = snapshot.sequence else {
        return false;
    };
    let symbol = snapshot.symbol.trim().to_ascii_uppercase();
    let gap = last_sequence_by_symbol
        .insert(symbol, sequence)
        .is_some_and(|previous| sequence <= previous);
    gap
}
