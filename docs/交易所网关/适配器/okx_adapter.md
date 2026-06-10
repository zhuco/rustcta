# OKX Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + Swap + Futures + Options REST adapter with public order book WS subscription metadata.

## Scope

- Public REST: Spot, Swap, Futures, and Options instruments and order book snapshots.
- Private REST: balances, derivative positions, fees, place order, quote market order for Spot, cancel order, cancel all through pending-order readback plus batch cancel, amend order, query order, open orders, and recent fills.
- Public funding REST: perpetual/futures funding rate readback through the shared `get_funding_rates` API on OKX mainnet.
- Account control: derivative leverage and account position mode are exposed through the shared `SetLeverageRequest` and `SetPositionModeRequest` APIs on OKX mainnet.
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
- `get_funding_rates`: native public REST `GET /api/v5/public/funding-rate` for derivative instruments.
- `set_leverage`: native private REST `POST /api/v5/account/set-leverage` for derivative instruments.
- `set_position_mode`: native private REST `POST /api/v5/account/set-position-mode`, mapping shared hedge mode to `long_short_mode` and one-way to `net_mode`.
- `batch_place_orders`: native REST `POST /api/v5/trade/batch-orders`, max 20, same market type required, partial exchange failure semantics.
- `batch_cancel_orders`: native REST `POST /api/v5/trade/cancel-batch-orders`, max 20, same market type required, partial exchange failure semantics.
- `cancel_all_orders`: composed from `GET /api/v5/trade/orders-pending` and `POST /api/v5/trade/cancel-batch-orders`.

## Signing

Private REST uses OKX HMAC-SHA256 Base64 signing over `timestamp + method + request_path + body`. Tests consume fixed signing vector fixtures and request-spec fixtures to assert path, method, headers, signed query/body fields, and secret redaction.

## Rate Limits And Pagination

The endpoint mapping declares public IP and private UID buckets. Recent fills support `begin`, `end`, and `limit`; adapter logic clamps limit to 1..100.

## Reconciliation And Safety

Private stream fallback uses REST `get_balances`, `get_positions`, `query_order`, `get_open_orders`, and `get_recent_fills`. Batch place/cancel returns per-item reports for partial success/failure and marks missing item responses for readback reconciliation. Account-control capabilities only advertise leverage and position-mode support when OKX mainnet private REST credentials are configured; `okxus` and `myokx` remain Spot-only profiles. Live dry-run eligibility requires upstream kill switch, disabled-symbol filtering, max-notional limits, and successful read-only reconciliation; this adapter does not require withdraw or transfer credentials.

## Recent Validation

```bash
cargo test -p rustcta-exchange-gateway okx_adapter_should_load_perpetual_funding_rate_from_public_rest -- --nocapture
cargo test -p rustcta-exchange-gateway okx_adapter_should_set_derivative_leverage_with_standard_account_control_request -- --nocapture
cargo test -p rustcta-exchange-gateway okx_adapter_should_set_position_mode_with_standard_account_control_request -- --nocapture
```

## WebSocket Policy

OKX public WS now builds Spot, Swap, Futures, and Options subscription sessions for `books5`, `books`, and `bbo-tbt`, with `seqId` parsed into order book sequence metadata. VIP/login-only TBT depth channels such as `books-l2-tbt` and `books50-l2-tbt` are documented but not enabled. Private WS remains unsupported.
