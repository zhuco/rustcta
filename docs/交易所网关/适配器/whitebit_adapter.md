# WhiteBIT Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + USDT perpetual REST adapter with public/private WebSocket request specs and private event normalizers.

## Scope

- Adapter id: `whitebit`
- Default REST base URL: `https://whitebit.com`
- Default WebSocket URL: `wss://api.whitebit.com/ws`
- Config env: `WHITEBIT_API_KEY`, `WHITEBIT_API_SECRET`, `WHITEBIT_PRIVATE_REST_ENABLED`
- Private REST signing: Base64 JSON payload in `X-TXC-PAYLOAD` and HMAC-SHA512 signature in `X-TXC-SIGNATURE`.
- Private WebSocket auth: signed REST call to `/api/v4/profile/websocket_token`, then `authorize` plus private subscription methods.

## Endpoint Mapping

The machine-readable mapping lives at `crates/rustcta-exchange-gateway/src/adapters/whitebit/endpoint_mapping.yaml`.

| Standard capability | WhiteBIT endpoint or method | Status |
| --- | --- | --- |
| Spot symbol rules | `GET /api/v4/public/markets` | Implemented |
| Perpetual symbol rules | `GET /api/v4/public/futures` | Implemented |
| Order book | `GET /api/v4/public/orderbook/{market}` | Implemented |
| Spot balances | `POST /api/v4/trade-account/balance` | Implemented |
| Perpetual balances | `POST /api/v4/collateral-account/balance` | Implemented |
| Perpetual positions | `POST /api/v4/collateral-account/positions/open` | Implemented |
| Spot orders | `POST /api/v4/order/new`, `POST /api/v4/order/market`, `POST /api/v4/order/cancel`, `POST /api/v4/order/status`, `POST /api/v4/orders` | Implemented |
| Perpetual orders | collateral account order, cancel, status, and open-order endpoints | Implemented |
| Spot batch orders | `POST /api/v4/order/bulk`, `POST /api/v4/order/cancel/bulk` | Implemented |
| Perpetual batch orders | `POST /api/v4/collateral-account/order/bulk`, bulk cancel endpoint | Implemented |
| Cancel all | `POST /api/v4/order/cancel/all` | Implemented |
| Recent fills | order/trade history endpoints | Implemented |
| Public WebSocket | `depth_subscribe`, `trades_subscribe`, `ticker_subscribe`, `candles_subscribe` | Subscription specs, heartbeat, parser helpers |
| Private WebSocket | `ordersPending_subscribe`, `deals_subscribe`, `balanceSpot_subscribe`, `balanceMargin_subscribe`, `positionsMargin_subscribe` | Token auth, subscription specs, private stream normalizer |
| Amend/order lists | no verified shared gateway mapping | Explicit `Unsupported` |

## Capabilities

The adapter populates legacy capability flags plus `capabilities_v2`:

- Stream runtime: public and private subscribe specs, client ping heartbeat, reconnect with token refresh/login and resubscribe, REST order-book resync after reconnect.
- Batch place: native, partial atomicity, max 20 items, one market type and one market pair per request.
- Batch cancel: native, partial atomicity, max 100 items, exchange order id or client order id required per item.
- History: open orders and recent fills support REST polling with limit/offset or time filters where exposed by the endpoint.
- Credential scope: read-only plus trade when private REST is enabled with credentials.

## Reconciliation And Safety

Timeouts or ambiguous order states must reconcile through `query_order`, `get_open_orders`, then `get_recent_fills`. Batch requests use native endpoints, but partial failures are not fully represented in the legacy response shape; failed or missing items require REST reconciliation before replay.

Live-dry-run must keep kill-switch, disabled-symbol, max-notional, and balance/order reconciliation gates enabled. Private WebSocket should be treated as a latency path, not the sole source of truth, until token renewal and reconnect behavior have live-readonly coverage.

## Validation

Targeted local validation:

```bash
cargo test -p rustcta-exchange-gateway whitebit --lib
```

The current tests cover public REST request specs/parsers, private REST signing and order routing, native batch place/cancel, private WebSocket token auth/subscriptions, heartbeat helpers, and private order/fill/balance/position event normalization.
