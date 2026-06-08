# BitMEX Gateway Adapter

Status date: 2026-06-08

Adapter id: `bitmex`

Implementation status: Spot and perpetual REST plus public/private WebSocket
request specs are implemented behind `rustcta_exchange_api::ExchangeClient`.
The Task 14 completion artifacts are BitMEX-only and include endpoint mapping,
capabilities v2 declarations, request/spec tests, signing vector coverage,
parser fixture coverage, WS runtime policy, rate limit, pagination,
reconciliation, batch, contract, and product-boundary documentation.

## Product Boundary

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST, private order lifecycle, public/private WS specs |
| Perpetual swap | `Perpetual` | Public REST, private order lifecycle, public/private WS specs |
| Dated futures | n/a | Documented unsupported until expiry/settlement contract metadata is modeled |
| Options | n/a | Unsupported; no options model is introduced |

Default REST base URL: `https://www.bitmex.com`

Default public/private WS URL: `wss://ws.bitmex.com/realtime`

Testnet is configurable through the adapter config base URLs.

## Authentication

Private REST uses these headers:

- `api-key`
- `api-expires`
- `api-signature`

The REST signature is lowercase hex `HMAC-SHA256(secret, METHOD + path +
expires + body)`.

Private WS uses `authKeyExpires` with signature
`HMAC-SHA256(secret, "GET/realtime" + expires)`. The runtime policy is to
re-login before expiry, and reconnect plus resubscribe if renewal fails.

## Endpoint Mapping

The machine-readable mapping lives in
`crates/rustcta-exchange-gateway/src/adapters/bitmex/endpoint_mapping.yaml`.

Key implemented endpoints:

| Capability | BitMEX endpoint/channel |
| --- | --- |
| symbol rules / contract spec | `GET /api/v1/instrument/active` |
| order book snapshot | `GET /api/v1/orderBook/L2` |
| balances | `GET /api/v1/user/margin` |
| positions | `GET /api/v1/position` |
| fees | `GET /api/v1/instrument/active` |
| place order | `POST /api/v1/order` |
| batch place | `POST /api/v1/order/bulk` |
| amend order | `PUT /api/v1/order` |
| cancel order / batch cancel | `DELETE /api/v1/order` |
| cancel all | `DELETE /api/v1/order/all` |
| query/open orders | `GET /api/v1/order` |
| recent fills | `GET /api/v1/execution/tradeHistory` |
| public WS | `orderBookL2:{symbol}`, `trade:{symbol}`, `quote:{symbol}`, `tradeBin*:{symbol}` |
| private WS | `order`, `execution`, `margin`, `position`, `wallet` |

## Capabilities V2

The adapter fills `ExchangeClientCapabilities::capabilities_v2` directly:

- public REST and public streams are native.
- private REST and private streams are native only when API credentials are
  configured.
- private WS without credentials is declared as REST fallback, not silently
  supported.
- batch place/cancel are native, partial, non-atomic bulk operations with a
  conservative `max_items=20`.
- order history supports limit-based open-order reconciliation with max 100.
- fill history supports `startTime`, `endTime`, and `count` with max 500.
- credential scopes are read-only and trade; no transfer or withdrawal scope is
  declared.

## WebSocket Runtime

Public/private connections are separate. Subscribe is supported; unsubscribe is
not declared in the shared capability.

Heartbeat policy:

- client sends `{"op":"ping"}` every 30 seconds.
- `{"op":"pong"}` or a payload with `pong` is accepted.
- stale or missing pong after 10 seconds triggers reconnect.
- reconnect requires resubscribe; private reconnect also requires re-login.

Order book strictness is `snapshot_only` for the shared gateway state because
the current parser normalizes `orderBookL2` pushes into snapshots and does not
declare strict sequence/checksum delta application. Consumers must fetch
`GET /api/v1/orderBook/L2` after reconnect or stale stream detection.

## Rate Limit And Pagination

The mapping declares exchange-header rate-limit handling using
`x-ratelimit-limit`, `x-ratelimit-remaining`, and `x-ratelimit-reset`.
Conservative buckets are split into public REST, private read, order, cancel,
and WebSocket scopes.

Pagination capability:

- open orders: `count` up to 100, reverse newest-first, filter by symbol/open.
- recent fills: `count` up to 500, supports `startTime` and `endTime`.
- cursors and from-id pagination are not declared.

## Reconciliation

Unknown order state is reconciled by querying `GET /api/v1/order` with
`orderID` or `clOrdID`, then checking open orders, then recent fills for
terminal fill evidence. Placement replay is disabled for unknown acknowledgement
states because BitMEX supports client order ids and duplicate replay can create
trading risk when ids are absent.

Live dry-run promotion requires the shared safety gates to be configured before
any private trading path is enabled:

- kill-switch gate
- disabled-symbol gate
- max-notional gate

## Funding And Open Interest

BitMEX exposes funding/open-interest style data through instrument fields, but
the current shared gateway trait has no typed BitMEX funding/open-interest
method. The mapping records candidate endpoints and declares these capabilities
unsupported for now.

## Unsupported Boundaries

- Quote-sized market orders are unsupported by the shared adapter surface.
- Standard OCO/OTO order lists are unsupported.
- Stop-limit is rejected because the unified request currently lacks separate
  limit price and stop trigger fields for BitMEX.
- Dated futures are not promoted to `MarketType::Futures` until contract expiry
  and settlement rules are represented.
- Options are explicitly not modeled.

## Validation

Targeted validation:

```bash
cargo test -p rustcta-exchange-gateway bitmex --lib
```

The BitMEX tests cover endpoint request specs, REST signing vectors, parser
fixtures for success/empty/error/missing-field cases, public/private WS payloads,
heartbeat, auth, runtime session behavior, capability v2 declarations, and the
endpoint mapping artifact.
