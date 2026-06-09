# Gate.io Gateway Adapter

`gateio` implements the RustCTA gateway `ExchangeClient` surface for Gate.io
Spot and USDT perpetual futures in
`crates/rustcta-exchange-gateway/src/adapters/gateio`.

## Scope

Supported:

- Spot public REST: currency pairs and order book snapshots.
- USDT perpetual public REST: contracts and order book snapshots.
- Spot private REST: balances, fees, place order, quote-sized market buy,
  cancel order, native cancel all by currency pair, amend order, query order,
  open orders, and recent fills.
- USDT perpetual private REST: balances, positions, fees, place order,
  cancel order, cancel all, query order, open orders, and recent fills.
- HMAC-SHA512 signing with `KEY`, `Timestamp`, and `SIGN` headers.

Project-unimplemented product boundaries:

- Delivery Futures and Options are `项目未实现`; the current runtime uses
  Gate.io Spot and `/futures/usdt` perpetual paths only. Promotion requires
  independent metadata, order book, account, position, risk and order lifecycle
  fixtures for Delivery Futures and Options.

Explicitly unsupported:

- Private WebSocket runtime wiring. REST snapshots and private REST readbacks
  are the declared fallback for reconciliation.

## Environment

```bash
RUSTCTA_GATEIO_REST_BASE_URL=https://api.gateio.ws/api/v4
GATEIO_API_KEY=...
GATEIO_API_SECRET=...
```

`GATE_API_KEY` and `GATE_API_SECRET` are accepted as fallbacks. Use read/trade
keys with withdrawal permission disabled.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/gateio/endpoint_mapping.yaml`.

| Capability | Spot endpoint | USDT perpetual endpoint |
| --- | --- | --- |
| symbol rules | `GET /spot/currency_pairs` | `GET /futures/usdt/contracts` |
| order book | `GET /spot/order_book` | `GET /futures/usdt/order_book` |
| balances | `GET /spot/accounts` | `GET /futures/usdt/accounts` |
| positions | `交易所不支持现货仓位` | `GET /futures/usdt/positions/{contract}` |
| fees | `GET /spot/fee` | `GET /futures/usdt/contracts/{contract}` |
| place / quote market order | `POST /spot/orders` | `POST /futures/usdt/orders` |
| cancel order | `DELETE /spot/orders/{order_id}` | `DELETE /futures/usdt/orders/{order_id}` |
| cancel all by symbol | `DELETE /spot/orders` | `DELETE /futures/usdt/orders` |
| amend order | `PATCH /spot/orders/{order_id}` | `交易所不支持当前共享 amend 映射` |
| query order | `GET /spot/orders/{order_id}` | `GET /futures/usdt/orders/{order_id}` |
| open orders | `GET /spot/open_orders` | `GET /futures/usdt/orders` |
| recent fills | `GET /spot/my_trades` | `GET /futures/usdt/my_trades` |

## Capability V2

- Product boundary: Spot + USDT perpetual. Delivery Futures and Options are
  explicit `project_unimplemented` boundaries in the mapping with a
  product-line source fixture; the existing perpetual runtime must not be used
  as Delivery/Options evidence.
- Public/private REST: native when credentials are configured for private REST.
- Public streams: Spot/USDT perpetual order book specs are declared. Private
  streams: `RestFallback`; the trait method returns `Unsupported`.
- Order book: snapshot-only, max depth 100; reconnect/resync uses REST
  snapshot.
- Recent fills pagination: `last_id` cursor and `limit <= 1000`.
- Batch place: composed sequential planner, non-atomic, max 20, partial failure
  possible. Cancel-all is native for a single currency pair and partial.

## Advanced Order Boundary

| Operation | Mapping status | Boundary |
| --- | --- | --- |
| `amend_order` | `native` | Spot amend is implemented with `PATCH /spot/orders/{order_id}`; futures amend is not mapped in the current shared runtime. |
| `place_order_list` | `unsupported` | No verified Gate.io OCO/order-list surface maps losslessly to the shared order-list model. |
| `batch_place_orders` | `composed` | Gateway planner composes sequential single-order REST calls; non-atomic with partial failures. |
| `batch_cancel_orders` | `native` | Symbol-scoped native multi-cancel/cancel-all style endpoint; partial failure semantics are explicit. |

## Runtime Policies

- Heartbeat/auth renewal: no gateway WebSocket session, so no heartbeat or
  renewal is active.
- Reconciliation: unknown order outcomes should query order, then open orders,
  then fills. Balances are read from private REST.
- Rate limits: mapping declares public, private, and order buckets; runtime
  should classify HTTP 429 and Gate.io rate-limit labels as retryable.
- Live dry-run gates: require reconciliation enabled, kill-switch, disabled
  symbol list, and max-notional limits before enabling any live private REST
  mutation.

## Fixtures And Tests

- Request-spec coverage is in `private_tests.rs` and asserts method, path,
  query/body, auth headers, and secret-free requests.
- Signing vector coverage is
  `gateio_signing_should_match_known_hmac_vector`.
- Parser fixtures live under `tests/fixtures/exchanges/gateio/toolchain/` and
  cover success, empty response, error response, and missing required fields.

## Validation

```bash
cargo test -p rustcta-exchange-gateway gateio --lib
python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/gateio/endpoint_mapping.yaml
python scripts/audit_gateway_adapters.py --exchange gateio
```
