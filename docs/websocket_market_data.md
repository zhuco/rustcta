# WebSocket Market Data

RustCTA uses a shared WebSocket-driven book cache for production arbitrage scans. REST polling remains available as a fallback/debug mode, but `spot_spot_taker_arbitrage` now defaults to `market_data_mode: websocket_cache`.

## Architecture

Core modules:

- `src/data/book_event.rs`: normalized book event model.
- `src/data/book_cache.rs`: concurrent latest-book cache keyed by exchange, market type, and internal symbol.
- `src/data/websocket_books.rs`: manager that subscribes through unified `ExchangeClient::subscribe_orderbook`.
- `src/data/book_health.rs`: public market-data health state.
- `src/data/book_recorder.rs`: nonblocking JSONL recorder.

Strategies and scanners consume these shared data modules. WebSocket ingestion
owns subscriptions and publishes nonblocking `BookEvent` updates. The
`spot_spot_taker_arbitrage` trading hot path consumes those events directly and
keeps a small in-memory top-of-book state for the affected symbol/pair; the
shared `BookCache` remains the read model for monitoring, preflight, debug, and
fallback reads.

## Normalization

MEXC Spot and CoinEx Spot use their unified public WebSocket order-book streams. Their exchange-specific parsers produce unified `OrderBookSnapshot` values. `WebSocketBookManager` converts those snapshots into `BookEvent` values and updates `BookCache`.

Each cached book stores:

- exchange and market type
- internal and exchange symbol
- bid/ask levels and best bid/ask
- exchange and local timestamps
- latency when the exchange timestamp is available
- sequence/update id when available
- source and stale flag

## Stale Rules

A book is stale when:

- the exchange stream reconnects or ends
- the heartbeat/message timeout fires
- the cached book age exceeds the configured stale threshold
- the parser marks the snapshot stale from exchange timestamp latency

Strategies must reject opportunities involving stale or missing books. `spot_spot_taker_arbitrage` records `StaleBook` instead of crashing or falling through to live orders.

## Reconnects

On stream end, subscribe error, or heartbeat timeout, the manager marks configured symbols stale and reconnects after `reconnect_interval_ms`. `max_reconnect_attempts: 0` means retry forever.

Sequence handling is best-effort. If a stream exposes a sequence/update id, non-monotonic updates increment the sequence gap metric and are logged. The current MEXC/CoinEx Spot path does not claim strict full-depth delta correctness.

## Recording

Book recording is JSONL and channel-based. WebSocket hot paths call `try_send`; if the channel is full, the event is dropped and `recorder_dropped_events` increments.

Default record mode is top-of-book only:

```json
{"event_id":"mexc-BTCUSDT-123-1780000000000000","event_kind":"snapshot","exchange":"mexc","market_type":"Spot","internal_symbol":"BTCUSDT","exchange_symbol":"BTCUSDT","best_bid":99999.0,"best_ask":100000.0,"exchange_timestamp":"2026-06-05T00:00:00Z","local_timestamp":"2026-06-05T00:00:00.001Z","received_at":"2026-06-05T00:00:00.001Z","latency_ms":1,"sequence":123,"source":"websocket"}
```

Set `record_top_of_book_only: false` to include full depth levels.

## Event-Driven Strategy Path

For `market_data_mode: websocket_cache`, `scan_interval_ms` is not the trading
decision trigger. Book events wake the spread engine, and only the updated
symbol's directed venue pairs are recomputed. If no book event arrives before
the interval elapses, the runtime uses that tick only for health, dashboard, and
report maintenance.

## Configuration

```yaml
market_data_mode: websocket_cache

websocket:
  enabled: true
  reconnect_interval_ms: 1000
  heartbeat_timeout_ms: 10000
  max_reconnect_attempts: 0
  record_books: true
  record_top_of_book_only: true
  book_recording_path: data/book_events.jsonl
  log_raw_messages: false

rest_polling:
  enabled: false
  interval_ms: 1000
```

## Tests

Offline tests:

```bash
cargo test --all-features
```

Ignored live public WebSocket tests:

```bash
ENABLE_LIVE_WS_TESTS=true cargo test --all-features --test live_websocket_books -- --ignored
```

The live tests use public MEXC/CoinEx streams only and do not require API keys.
