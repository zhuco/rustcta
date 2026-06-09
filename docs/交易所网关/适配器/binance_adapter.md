# Binance Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + USD-M REST adapter with public order book WS subscription metadata and USD-M native batch order support.

## Scope

- Public REST: Spot and USD-M symbol rules and order book snapshots.
- Private REST: balances, fees, place order, quote market order, cancel order, cancel all, amend order, OCO order list, query order, open orders, USD-M positions, USD-M batch place/cancel, and recent fills.
- Public WebSocket: Spot/USD-M depth and bookTicker subscription payloads are wired through `subscribe_public_stream`; REST depth snapshots remain the resync source.
- Private WebSocket: not wired in this gateway adapter; REST reconciliation is the fallback.
- Product boundary: COIN-M inverse contracts are delegated to `binancecoinm`; Binance Options are official but project-unimplemented in this Spot + USD-M profile.

## Toolchain Artifacts

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/binance/endpoint_mapping.yaml`.
- Request specs: `tests/fixtures/exchanges/binance/request_specs/`.
- Signing vector: `tests/fixtures/exchanges/binance/signing_vectors/place_order_limit.json`.
- Parser fixtures: `tests/fixtures/exchanges/binance/parser/`.
- WS fixtures: `tests/fixtures/exchanges/binance/ws/`.
- `capabilities_v2` declaration: `crates/rustcta-exchange-gateway/src/adapters/binance/toolchain.rs`.

## Capabilities

- `public_rest`: native.
- `private_rest`: native when `BINANCE_SPOT_PRIVATE_REST_ENABLED` is true and key/secret are present.
- `public_streams`: REST-fallback runtime metadata for `bookTicker`, partial depth `5/10/20`, and diff depth `@100ms`; resync uses `GET /api/v3/depth` or `/fapi/v1/depth`.
- `private_streams`: unsupported in runtime; use REST reconciliation.
- `batch_place_orders`: native USD-M `/fapi/v1/batchOrders`, partial exchange failure semantics, max 5; Spot remains unsupported for shared native batch.
- `batch_cancel_orders`: native USD-M `/fapi/v1/batchOrders`, partial exchange failure semantics, max 10, same symbol required; Spot remains unsupported for shared native batch.
- `cancel_all_orders`: native REST `DELETE /api/v3/openOrders`, partial exchange failure semantics.

## Signing

Private REST uses Binance HMAC-SHA256 hex signing over the canonical query string. Tests consume the fixed signing vector fixture and request-spec fixtures to assert path, method, auth header, signed query fields, and secret redaction.

## Rate Limits And Pagination

The endpoint mapping declares public IP, signed UID, and order-count buckets with per-endpoint weights. Recent fills support `startTime`, `endTime`, `fromId`, and `limit` up to 1000.

## Reconciliation And Safety

Private stream fallback uses REST `get_balances`, `query_order`, `get_open_orders`, and `get_recent_fills`. USD-M batch place/cancel returns per-item reports for partial success/failure and marks missing item responses for readback reconciliation. Live dry-run eligibility requires upstream kill switch, disabled-symbol filtering, max-notional limits, and successful read-only reconciliation; this adapter does not require withdraw or transfer credentials.

## WebSocket Policy

Binance public WS now builds Spot and USD-M subscription sessions for `@bookTicker`, `@depth5/10/20[@100ms]`, and `@depth[@100ms]`. Sequence evidence is parsed from `lastUpdateId` or `u`; gap handling still requires REST snapshot plus buffered delta replay. Private user data streams remain unsupported.
