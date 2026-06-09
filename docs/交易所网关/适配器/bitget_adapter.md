# Bitget Gateway Adapter

`bitget` implements the RustCTA gateway `ExchangeClient` surface for Bitget
Spot and USDT-FUTURES in `crates/rustcta-exchange-gateway/src/adapters/bitget`.

## Scope

Supported:

- Spot public REST: symbol rules and order book snapshots.
- USDT-FUTURES public REST: contract rules and order book snapshots.
- Spot private REST: balances, fees, place order, quote-sized market buy,
  cancel order, cancel all by symbol, amend order, query order, open orders,
  and recent fills.
- USDT-FUTURES private REST: balances, positions, fees, place order,
  cancel order, cancel all, amend order, query order, open orders, and
  recent fills.
- HMAC-SHA256 Base64 signing with `ACCESS-*` headers.

Explicitly unsupported:

- USDC-FUTURES, COIN-FUTURES, and Margin/UTA product lines are
  `项目未实现`; the current runtime hardcodes Bitget futures requests to
  `USDT-FUTURES`.
- Private WebSocket runtime wiring. REST snapshots and private REST readbacks
  are the declared fallback for reconciliation.

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

| Capability | Spot endpoint | USDT-FUTURES endpoint |
| --- | --- | --- |
| symbol rules | `GET /api/v2/spot/public/symbols` | `GET /api/v2/mix/market/contracts` |
| order book | `GET /api/v2/spot/market/orderbook` | `GET /api/v2/mix/market/orderbook` |
| balances | `GET /api/v2/spot/account/assets` | `GET /api/v2/mix/account/accounts` |
| fees | `GET /api/v3/account/fee-rate` | `GET /api/v2/mix/market/contracts` |
| place / quote market order | `POST /api/v2/spot/trade/place-order` | `POST /api/v2/mix/order/place-order` |
| cancel order | `POST /api/v2/spot/trade/cancel-order` | `POST /api/v2/mix/order/cancel-order` |
| cancel all by symbol | `POST /api/v2/spot/trade/cancel-symbol-order` | `POST /api/v2/mix/order/cancel-all-orders` |
| amend order | `POST /api/v3/trade/modify-order` | `POST /api/v2/mix/order/modify-order` |
| query order | `GET /api/v2/spot/trade/orderInfo` | `GET /api/v2/mix/order/detail` |
| open orders | `GET /api/v2/spot/trade/unfilled-orders` | `GET /api/v2/mix/order/orders-pending` |
| recent fills | `GET /api/v2/spot/trade/fills` | `GET /api/v2/mix/order/fills` |

## Capability V2

- Product boundary: Spot + USDT-FUTURES. USDC-FUTURES, COIN-FUTURES, and
  Margin/UTA are explicit `project_unimplemented` boundaries in the mapping.
- 2026-06-09 product-line audit: Bitget official Futures API has
  `USDT-FUTURES`, `USDC-FUTURES`, and `COIN-FUTURES` product types, and Bitget
  documents a separate Spot-Margin API. The gateway keeps USDC-FUTURES,
  COIN-FUTURES, and Margin as source-boundary project gaps until productType
  and settlement-coin guards, metadata/books, account/position or liability
  parsers, signed order specs, and post-write reconciliation are added.
- Public/private REST: native when credentials are configured for private REST.
- Public streams: Spot/USDT-FUTURES order book specs are declared. Private
  streams: `RestFallback`; the trait method returns `Unsupported`.
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
