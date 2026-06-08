# CoinEx Adapter Migration

Task 11 scope: `coinex` is Spot-only. The adapter normalizes symbols to compact
uppercase venue symbols (`BTC/USDT` -> `BTCUSDT`) for REST and WebSocket specs.

Futures is 项目未实现, not `交易所不支持合约`: CoinEx official API v2
documents separate Spot and Futures REST/WebSocket modules. Current `coinex`
adapter only maps Spot.

## Coverage

| Area | Status |
| --- | --- |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/coinex/endpoint_mapping.yaml` |
| Capabilities v2 | Declares public/private REST, public/private stream policy, cursor/limit history, composed non-native batch place/cancel |
| Request-spec tests | `public_tests.rs`, `private_tests.rs`, `stream_tests.rs` assert paths, methods, symbols and signed headers |
| Signing vectors | `private_tests.rs` and `stream_tests.rs` cover REST HMAC headers and WS login signature shape |
| Parser fixtures | External JSON fixtures cover success, empty/error paths through parser and transport tests |
| Public WS | Spec helper covers subscribe payloads, heartbeat and REST snapshot resync policy |
| Private WS | Spec helper covers `server.sign` login; private events require REST reconciliation fallback |
| Pagination | Open orders and fills declare limit pagination with max 1000 |
| Reconciliation | Query order after unknown place/cancel; REST snapshot after WS order book gap |
| Batch | Place and cancel are composed sequential/non-atomic with partial failure, max 10 items |

## Public WebSocket Order Book

Official Spot WS supports `depth.subscribe` on `wss://socket.coinex.com/v2/spot`. The depth feed can be full or incremental, pushes roughly every 200ms when changed, sends full market depth around every 1 minute, supports limit 5/10/20/50, and carries a signed 32-bit CRC32 checksum. Implementation should structure `market_list`, limit, merge interval, `is_full`, and checksum verification before relying on the feed for arbitrage.

## Safety Boundary

No real credentials or account identifiers are committed. Private WS is declared
as spec-level plus REST reconciliation fallback; full event parser activation is
left behind explicit private credential configuration.
