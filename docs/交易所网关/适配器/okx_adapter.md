# OKX Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + Swap + Futures + Options REST adapter with public order book WS subscription metadata.

## Scope

- Public REST: Spot, Swap, Futures, and Options instruments and order book snapshots.
- Private REST: balances, derivative positions, fees, place order, quote market order for Spot, cancel order, cancel all through pending-order readback plus batch cancel, amend order, query order, open orders, and recent fills.
- Public WebSocket: Spot/Swap/Futures/Options `books5`, `books`, and `bbo-tbt` subscription payloads are wired through `subscribe_public_stream`; REST order book snapshots are the resync source.
- Private WebSocket: not wired in this gateway adapter; REST reconciliation is the fallback.

## Toolchain Artifacts

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/okx/endpoint_mapping.yaml`.
- Request specs: `tests/fixtures/exchanges/okx/request_specs/`.
- Signing vectors: `tests/fixtures/exchanges/okx/signing_vectors/`.
- Parser fixtures: `tests/fixtures/exchanges/okx/parser/`.
- WS fixtures: `tests/fixtures/exchanges/okx/ws/`.
- `capabilities_v2` declaration: `crates/rustcta-exchange-gateway/src/adapters/okx/toolchain.rs`.

## Capabilities

- `public_rest`: native.
- `private_rest`: native when key, secret, and passphrase are present and private REST is enabled.
- `public_streams`: REST-fallback runtime metadata for Spot/Swap/Futures/Options `books5`, `books`, and `bbo-tbt`; resync uses `GET /api/v5/market/books`.
- `private_streams`: unsupported in runtime; use REST reconciliation.
- `get_positions`: native REST `GET /api/v5/account/positions` for Swap/Futures; Spot positions remain unsupported.
- `batch_place_orders`: native REST `POST /api/v5/trade/batch-orders`, max 20, same market type required, partial exchange failure semantics.
- `batch_cancel_orders`: native REST `POST /api/v5/trade/cancel-batch-orders`, max 20, same market type required, partial exchange failure semantics.
- `cancel_all_orders`: composed from `GET /api/v5/trade/orders-pending` and `POST /api/v5/trade/cancel-batch-orders`.

## Signing

Private REST uses OKX HMAC-SHA256 Base64 signing over `timestamp + method + request_path + body`. Tests consume fixed signing vector fixtures and request-spec fixtures to assert path, method, headers, signed query/body fields, and secret redaction.

## Rate Limits And Pagination

The endpoint mapping declares public IP and private UID buckets. Recent fills support `begin`, `end`, and `limit`; adapter logic clamps limit to 1..100.

## Reconciliation And Safety

Private stream fallback uses REST `get_balances`, `get_positions`, `query_order`, `get_open_orders`, and `get_recent_fills`. Batch place/cancel returns per-item reports for partial success/failure and marks missing item responses for readback reconciliation. Live dry-run eligibility requires upstream kill switch, disabled-symbol filtering, max-notional limits, and successful read-only reconciliation; this adapter does not require withdraw or transfer credentials.

## WebSocket Policy

OKX public WS now builds Spot, Swap, Futures, and Options subscription sessions for `books5`, `books`, and `bbo-tbt`, with `seqId` parsed into order book sequence metadata. VIP/login-only TBT depth channels such as `books-l2-tbt` and `books50-l2-tbt` are documented but not enabled. Private WS remains unsupported.
