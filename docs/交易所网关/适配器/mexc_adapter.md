# MEXC Gateway Adapter

`mexc` implements the RustCTA gateway `ExchangeClient` surface for MEXC Spot
REST in `crates/rustcta-exchange-gateway/src/adapters/mexc`.

## Scope

Supported:

- Spot public REST: exchange info and order book snapshots.
- Spot private REST: balances, fees, place order, quote-sized market buy,
  cancel order, native cancel all by symbol, query order, open orders, and
  recent fills.
- Binance-like HMAC-SHA256 query signing with `X-MEXC-APIKEY`.

Explicitly unsupported:

- Non-Spot markets in this gateway adapter.
- Amend order and order-list/OCO methods in the shared trait.
- Public/private WebSocket runtime wiring. REST snapshots and private REST
  readbacks are the declared fallback for resync and reconciliation.

## Environment

```bash
RUSTCTA_MEXC_REST_BASE_URL=https://api.mexc.com
MEXC_API_KEY=...
MEXC_API_SECRET=...
```

Use read/trade keys with withdrawal permission disabled.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/mexc/endpoint_mapping.yaml`.

| Capability | Endpoint |
| --- | --- |
| symbol rules | `GET /api/v3/exchangeInfo` |
| order book | `GET /api/v3/depth` |
| balances | `GET /api/v3/account` |
| fees | `GET /api/v3/tradeFee` |
| place / quote market order | `POST /api/v3/order` |
| cancel order | `DELETE /api/v3/order` |
| cancel all by symbol | `DELETE /api/v3/openOrders` |
| query order | `GET /api/v3/order` |
| open orders | `GET /api/v3/openOrders` |
| recent fills | `GET /api/v3/myTrades` |

## Capability V2

- Product boundary: Spot only.
- Public/private REST: native when credentials are configured for private REST.
- Public/private streams: `RestFallback`; the trait methods return
  `Unsupported`.
- Order book: snapshot-only, max depth 50; reconnect/resync uses REST snapshot.
- Recent fills pagination: `fromId`, `startTime`, `endTime`, `limit <= 1000`.
- Batch place: composed sequential planner, non-atomic, max 20, partial failure
  possible. Cancel-all is native for a single symbol and partial.

## Runtime Policies

- Heartbeat/auth renewal: no gateway WebSocket session, so no heartbeat or
  renewal is active.
- Reconciliation: unknown order outcomes should query order, then open orders,
  then fills. Balances are read from private REST.
- Rate limits: mapping declares public, private, and order buckets; runtime
  should classify HTTP 429 and MEXC rate-limit codes as retryable.
- Live dry-run gates: require reconciliation enabled, kill-switch, disabled
  symbol list, and max-notional limits before enabling any live private REST
  mutation.

## Fixtures And Tests

- Request-spec coverage is in `private_tests.rs` and asserts method, path,
  query/body, auth header, recvWindow/timestamp/signature, and secret-free
  requests.
- Signing vector coverage is `mexc_signing_should_match_known_hmac`, backed by
  `tests/fixtures/exchanges/mexc/toolchain/signing_vector.json`.
- Parser fixtures live under `tests/fixtures/exchanges/mexc/toolchain/` and
  cover success, empty response, error response, and missing required fields.

## Validation

```bash
cargo test -p rustcta-exchange-gateway mexc --lib
python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/mexc/endpoint_mapping.yaml
python scripts/audit_gateway_adapters.py --exchange mexc
```
