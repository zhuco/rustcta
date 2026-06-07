# KuCoin Adapter Migration

Task 11 scope: `kucoin` is Spot-only. The adapter normalizes symbols to dashed
uppercase venue symbols (`BTC/USDT` -> `BTC-USDT`) for REST and WebSocket specs.

## Coverage

| Area | Status |
| --- | --- |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/kucoin/endpoint_mapping.yaml` |
| Capabilities v2 | Declares public/private REST, bullet-token private streams, stream auth renewal, cursor/limit fills history, composed batch planner |
| Request-spec tests | `public_tests.rs`, `private_tests.rs`, `stream_tests.rs` assert paths, methods, symbols and signed KuCoin headers |
| Signing vectors | `tests/fixtures/exchanges/kucoin/signing_vectors/` covers HMAC-SHA256 base64; stream tests cover bullet token lease and renewal policy |
| Parser fixtures | External JSON fixtures and request-spec fixtures cover public rules/books and private readback/mutation responses |
| Public WS | Spec helper covers topics, subscribe/unsubscribe payloads and ping/pong response |
| Private WS | `/api/v1/bullet-private` token lease with refresh-before-expiry and resubscribe-after-renewal policy |
| Pagination | Open orders/fills declare cursor+limit; fills support since/until, max 500 |
| Reconciliation | Query order after unknown place/cancel; REST snapshot after WS order book gap |
| Batch | Composed sequential/non-atomic place and cancel, max 20, REST reconciliation required |

## Coordination Notes

KuCoin token renewal is represented with existing `StreamAuthCapability` fields
and adapter-local `KuCoinBulletTokenLease`. If shared task 5 later exposes richer
lease metadata, this adapter can map the same helper into the shared runtime
without changing private REST behavior.
