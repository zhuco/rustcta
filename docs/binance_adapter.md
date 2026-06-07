# Binance Gateway Adapter

Status: `rustcta-exchange-gateway` Spot REST adapter with task-9 toolchain metadata.

## Scope

- Public REST: symbol rules and order book snapshots.
- Private REST: balances, fees, place order, quote market order, cancel order, cancel all, amend order, OCO order list, query order, open orders, and recent fills.
- Public WebSocket: not wired in this gateway adapter; REST order book snapshots are the current resync source.
- Private WebSocket: not wired in this gateway adapter; REST reconciliation is the fallback.

## Toolchain Artifacts

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/binance/endpoint_mapping.yaml`.
- Request specs: `tests/fixtures/exchanges/binance/request_specs/`.
- Signing vector: `tests/fixtures/exchanges/binance/signing_vectors/place_order_limit.json`.
- Parser fixtures: `tests/fixtures/exchanges/binance/parser/`.
- `capabilities_v2` declaration: `crates/rustcta-exchange-gateway/src/adapters/binance/toolchain.rs`.

## Capabilities

- `public_rest`: native.
- `private_rest`: native when `BINANCE_SPOT_PRIVATE_REST_ENABLED` is true and key/secret are present.
- `public_streams`: unsupported in runtime.
- `private_streams`: unsupported in runtime; use REST reconciliation.
- `batch_place_orders`: unsupported.
- `batch_cancel_orders`: unsupported.
- `cancel_all_orders`: native REST `DELETE /api/v3/openOrders`, partial exchange failure semantics.

## Signing

Private REST uses Binance HMAC-SHA256 hex signing over the canonical query string. Tests consume the fixed signing vector fixture and request-spec fixtures to assert path, method, auth header, signed query fields, and secret redaction.

## Rate Limits And Pagination

The endpoint mapping declares public IP, signed UID, and order-count buckets with per-endpoint weights. Recent fills support `startTime`, `endTime`, `fromId`, and `limit` up to 1000.

## Reconciliation And Safety

Private stream fallback uses REST `get_balances`, `query_order`, `get_open_orders`, and `get_recent_fills`. Live dry-run eligibility requires upstream kill switch, disabled-symbol filtering, max-notional limits, and successful read-only reconciliation; this adapter does not require withdraw or transfer credentials.

## WebSocket Policy

Binance public WS heartbeat/listen-key semantics are recorded in endpoint mapping for future runtime work, but current `subscribe_public_stream` and `subscribe_private_stream` return `Unsupported`. Order book resync uses REST snapshots.
