# OKX Gateway Adapter

Status: `rustcta-exchange-gateway` Spot REST adapter with task-9 toolchain metadata.

## Scope

- Public REST: Spot instruments and order book snapshots.
- Private REST: balances, fees, place order, quote market order, cancel order, cancel all through pending-order readback plus batch cancel, amend order, query order, open orders, and recent fills.
- Public WebSocket: not wired in this gateway adapter; REST order book snapshots are the current resync source.
- Private WebSocket: not wired in this gateway adapter; REST reconciliation is the fallback.

## Toolchain Artifacts

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/okx/endpoint_mapping.yaml`.
- Request specs: `tests/fixtures/exchanges/okx/request_specs/`.
- Signing vectors: `tests/fixtures/exchanges/okx/signing_vectors/`.
- Parser fixtures: `tests/fixtures/exchanges/okx/parser/`.
- `capabilities_v2` declaration: `crates/rustcta-exchange-gateway/src/adapters/okx/toolchain.rs`.

## Capabilities

- `public_rest`: native.
- `private_rest`: native when key, secret, and passphrase are present and private REST is enabled.
- `public_streams`: unsupported in runtime.
- `private_streams`: unsupported in runtime; use REST reconciliation.
- `batch_place_orders`: unsupported.
- `batch_cancel_orders`: unsupported as a public trait operation; native cancel-batch is used internally by `cancel_all_orders`.
- `cancel_all_orders`: composed from `GET /api/v5/trade/orders-pending` and `POST /api/v5/trade/cancel-batch-orders`.

## Signing

Private REST uses OKX HMAC-SHA256 Base64 signing over `timestamp + method + request_path + body`. Tests consume fixed signing vector fixtures and request-spec fixtures to assert path, method, headers, signed query/body fields, and secret redaction.

## Rate Limits And Pagination

The endpoint mapping declares public IP and private UID buckets. Recent fills support `begin`, `end`, and `limit`; adapter logic clamps limit to 1..100.

## Reconciliation And Safety

Private stream fallback uses REST `get_balances`, `query_order`, `get_open_orders`, and `get_recent_fills`. Live dry-run eligibility requires upstream kill switch, disabled-symbol filtering, max-notional limits, and successful read-only reconciliation; this adapter does not require withdraw or transfer credentials.

## WebSocket Policy

OKX public WS heartbeat and private login semantics are recorded in endpoint mapping for future runtime work, but current `subscribe_public_stream` and `subscribe_private_stream` return `Unsupported`. Order book resync uses REST snapshots.
