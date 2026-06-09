# KuCoin Adapter Migration

`kucoin` is the Spot adapter. It normalizes symbols to dashed uppercase venue
symbols (`BTC/USDT` -> `BTC-USDT`) for REST and WebSocket specs.

Futures are not implemented in this `kucoin` spot adapter, but they are not
`交易所不支持合约`: KuCoin official docs expose Futures REST/WS and the project
has a separate `kucoinfutures` adapter/profile for that API family.

## Coverage

| Area | Status |
| --- | --- |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/kucoin/endpoint_mapping.yaml` |
| Capabilities v2 | Declares Spot public/private REST, native public streams, bullet-token private stream auth metadata, cursor/limit fills history, and composed batch planner |
| Public WS | OBU `depth=1` BBO, `depth=5/50` 100ms, and `depth=increment` real-time are mapped with sequence fields and REST snapshot replay |
| Private WS | `/api/v1/bullet-private` token lease with refresh-before-expiry and resubscribe-after-renewal policy |
| Fees | Signed `GET /api/v1/trade-fees` is mapped to `get_fees` with read-only credential scope and request-spec fixture |
| Request-spec tests | `public_tests.rs`, `private_tests.rs`, `stream_tests.rs` assert paths, methods, symbols, signed KuCoin headers, OBU payloads, and fee request specs |
| Signing vectors | `tests/fixtures/exchanges/kucoin/signing_vectors/` covers HMAC-SHA256 base64; stream tests cover bullet token lease and renewal policy |
| Parser fixtures | External JSON fixtures and request-spec fixtures cover public rules/books, OBU deltas/snapshots, and private readback/mutation responses |
| Pagination | Open orders/fills declare cursor+limit; fills support since/until, max 500 |
| Reconciliation | Query order after unknown place/cancel; REST snapshot after WS order book gap |
| Batch | Composed sequential/non-atomic place and cancel, max 20, REST reconciliation required |

## WebSocket 行情

| 通道 | 产品线 | 状态 | 推流间隔 | 档位 | 序列/校验 | 重建策略 |
| --- | --- | --- | --- | --- | --- | --- |
| OBU `depth=1` | Spot | 已补 | real-time no fixed ms | 1 | `sequence` / no checksum | REST snapshot fallback |
| OBU `depth=5` | Spot | 已补 | 100ms | 5 | `sequence` / no checksum | REST snapshot fallback |
| OBU `depth=50` | Spot | 已补 | 100ms | 50 | `sequence` / no checksum | REST snapshot fallback |
| OBU `depth=increment` | Spot | 已补 | real-time no fixed ms | incremental | `sequenceStart`/`sequenceEnd` / no checksum | REST snapshot + delta replay |

KuCoin UTA `depth=increment@10ms` with 500 levels is documented as a future
migration effective 2026-06-17 UTC. It is not marked as currently available.

## Coordination Notes

KuCoin token renewal is represented with existing `StreamAuthCapability` fields
and adapter-local `KuCoinBulletTokenLease`. If shared task 5 later exposes richer
lease metadata, this adapter can map the same helper into the shared runtime
without changing private REST behavior.
