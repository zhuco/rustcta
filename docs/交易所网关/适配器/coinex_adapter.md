# CoinEx Adapter Migration

Task 11 scope: `coinex` is Spot-only. The adapter normalizes symbols to compact
uppercase venue symbols (`BTC/USDT` -> `BTCUSDT`) for REST and WebSocket specs.

Futures is 项目未实现, not `交易所不支持合约`: CoinEx official API v2
documents separate Spot and Futures REST/WebSocket modules. Current `coinex`
adapter only maps Spot.

2026-06-09 产品线边界收窄：`contract_product` 与 `futures_product`
均标为 `project_unimplemented`，source boundary 为
`tests/fixtures/exchanges/coinex/request_specs/product_line_source_boundary.json`。
Futures/Perpetual 不走当前 Spot v2 host、Spot balances、Spot order parser 或 Spot
WS session；补齐前需要独立 futures market metadata、leverage/risk、positions 和
order lifecycle runtime。
状态建议：继续保留 `contract_product` / `futures_product = 项目未实现`；Futures
API 是单独产品线，不能写成 `不支持`，也不能用 Spot v2 下单、余额或盘口路径承载
futures/perpetual runtime。

## Coverage

| Area | Status |
| --- | --- |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/coinex/endpoint_mapping.yaml` |
| Capabilities v2 | Declares public/private REST, public/private stream policy, cursor/limit history, composed non-native batch place/cancel |
| Request-spec tests | `public_tests.rs`, `private_tests.rs`, `stream_tests.rs` assert paths, methods, symbols and signed headers |
| Signing vectors | `private_tests.rs` and `stream_tests.rs` cover REST HMAC headers and WS login signature shape |
| Parser fixtures | External JSON fixtures cover success, empty/error paths through parser and transport tests |
| Public WS | Spec helper covers `depth.subscribe` `market_list`, heartbeat, REST snapshot resync policy, full/incremental order-book mode, merge interval, and CRC32 checksum |
| Private WS | Spec helper covers `server.sign` login; private events require REST reconciliation fallback |
| Pagination | Open orders and fills declare limit pagination with max 1000 |
| Reconciliation | Query order after unknown place/cancel; REST snapshot after WS order book gap |
| Batch | Place and cancel are composed sequential/non-atomic with partial failure, max 10 items |

## Public WebSocket Order Book

Official Spot WS supports `depth.subscribe` on `wss://socket.coinex.com/v2/spot`.

Structured details now covered:

| Field | CoinEx detail |
| --- | --- |
| Subscribe method | `depth.subscribe` |
| Push method | `depth.update` |
| Params shape | `{"market_list": [[market, limit, interval, is_full]]}` |
| Cadence | About 200ms when changed |
| Limit | `5`, `10`, `20`, `50`; default helper uses `50` |
| Merge interval | Default `0`; supported values are recorded in `endpoint_mapping.yaml` |
| Full/incremental | `is_full=true` sends full subscribed depth; `is_full=false` sends changed levels and quantity `0` deletes a level |
| Full refresh | Full market depth is delivered about every 1 minute |
| Checksum | CRC32 over `bid1_price:bid1_amount:...:ask1_price:ask1_amount...`, accepted as signed i32 or equivalent unsigned wire value |
| Resync | Build from REST `/spot/depth`; rebuild/resubscribe after reconnect, stale stream, parse error, or checksum mismatch |

Focused coverage lives in `stream_tests.rs` with sanitized fixtures under `tests/fixtures/exchanges/coinex/ws/`.

## Safety Boundary

No real credentials or account identifiers are committed. Private WS is declared
as spec-level plus REST reconciliation fallback; full event parser activation is
left behind explicit private credential configuration.
