# Bitget Gateway Adapter

`bitget` implements the RustCTA gateway `ExchangeClient` surface for Bitget
Spot REST in `crates/rustcta-exchange-gateway/src/adapters/bitget`.

## Scope

Supported:

- Spot public REST: symbol rules and order book snapshots.
- Spot private REST: balances, fees, place order, quote-sized market buy,
  cancel order, cancel all by symbol, amend order, query order, open orders,
  and recent fills.
- HMAC-SHA256 Base64 signing with `ACCESS-*` headers.

Explicitly unsupported:

- Perpetual/futures trading in this gateway adapter. Existing legacy Bitget
  perp fixtures are not completion evidence for this Spot adapter.
- Public/private WebSocket runtime wiring. REST snapshots and private REST
  readbacks are the declared fallback for resync and reconciliation.

## Environment

```bash
RUSTCTA_BITGET_REST_BASE_URL=https://api.bitget.com
BITGET_API_KEY=...
BITGET_API_SECRET=...
BITGET_API_PASSPHRASE=...
```

Use read/trade-only keys. Withdrawal permission must be disabled.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/bitget/endpoint_mapping.yaml`.

| Capability | Endpoint |
| --- | --- |
| symbol rules | `GET /api/v2/spot/public/symbols` |
| order book | `GET /api/v2/spot/market/orderbook` |
| balances | `GET /api/v2/spot/account/assets` |
| fees | `GET /api/v3/account/fee-rate` |
| place / quote market order | `POST /api/v2/spot/trade/place-order` |
| cancel order | `POST /api/v2/spot/trade/cancel-order` |
| cancel all by symbol | `POST /api/v2/spot/trade/cancel-symbol-order` |
| amend order | `POST /api/v3/trade/modify-order` |
| query order | `GET /api/v2/spot/trade/orderInfo` |
| open orders | `GET /api/v2/spot/trade/unfilled-orders` |
| recent fills | `GET /api/v2/spot/trade/fills` |

## Capability V2

- Product boundary: Spot only.
- Public/private REST: native when credentials are configured for private REST.
- Public/private streams: `RestFallback`; the trait methods return
  `Unsupported`.
- Order book: snapshot-only, max depth 50; reconnect/resync uses REST snapshot.
- Recent fills pagination: `idLessThan`, `startTime`, `endTime`, `limit <= 100`.
- Batch place/cancel: composed sequential planner, non-atomic, max 20, partial
  failure possible. Native cancel-all is partial by symbol.

## Runtime Policies

- Heartbeat/auth renewal: no gateway WebSocket session, so no heartbeat or
  renewal is active.
- Reconciliation: unknown order outcomes should query order, then open orders,
  then fills. Balances are read from private REST.
- Rate limits: mapping declares public, private, and order buckets; runtime
  should classify HTTP 429 and Bitget rate-limit messages as retryable.
- Live dry-run gates: require reconciliation enabled, kill-switch, disabled
  symbol list, and max-notional limits before enabling any live private REST
  mutation.

## Fixtures And Tests

- Request-spec coverage is in `private_tests.rs` and asserts method, path,
  query/body, auth headers, and secret-free requests.
- Signing vector coverage is
  `bitget_signing_should_match_known_hmac_vector`.
- Parser fixtures live under `tests/fixtures/exchanges/bitget/toolchain/` and
  cover success, empty response, error response, and missing required fields.

## Validation

```bash
cargo test -p rustcta-exchange-gateway bitget --lib
python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitget/endpoint_mapping.yaml
python scripts/audit_gateway_adapters.py --exchange bitget
```
