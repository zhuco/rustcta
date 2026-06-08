# Gate.io Gateway Adapter

`gateio` implements the RustCTA gateway `ExchangeClient` surface for Gate.io
Spot REST in `crates/rustcta-exchange-gateway/src/adapters/gateio`.

## Scope

Supported:

- Spot public REST: currency pairs and order book snapshots.
- Spot private REST: balances, fees, place order, quote-sized market buy,
  cancel order, native cancel all by currency pair, amend order, query order,
  open orders, and recent fills.
- HMAC-SHA512 signing with `KEY`, `Timestamp`, and `SIGN` headers.

Explicitly unsupported:

- Non-Spot markets in this gateway adapter.
- Public/private WebSocket runtime wiring. REST snapshots and private REST
  readbacks are the declared fallback for resync and reconciliation.

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

| Capability | Endpoint |
| --- | --- |
| symbol rules | `GET /spot/currency_pairs` |
| order book | `GET /spot/order_book` |
| balances | `GET /spot/accounts` |
| fees | `GET /spot/fee` |
| place / quote market order | `POST /spot/orders` |
| cancel order | `DELETE /spot/orders/{order_id}` |
| cancel all by symbol | `DELETE /spot/orders` |
| amend order | `PATCH /spot/orders/{order_id}` |
| query order | `GET /spot/orders/{order_id}` |
| open orders | `GET /spot/open_orders` |
| recent fills | `GET /spot/my_trades` |

## Capability V2

- Product boundary: Spot only.
- Public/private REST: native when credentials are configured for private REST.
- Public/private streams: `RestFallback`; the trait methods return
  `Unsupported`.
- Order book: snapshot-only, max depth 100; reconnect/resync uses REST
  snapshot.
- Recent fills pagination: `last_id` cursor and `limit <= 1000`.
- Batch place: composed sequential planner, non-atomic, max 20, partial failure
  possible. Cancel-all is native for a single currency pair and partial.

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
