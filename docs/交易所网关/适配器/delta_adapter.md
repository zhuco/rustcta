# Delta Exchange Gateway Adapter

Task 4 implements the Delta Exchange half as a REST-first derivatives adapter.
The adapter is intentionally not registered in `adapters/mod.rs` or `lib.rs`;
the main integration agent should add shared registration after reviewing the
Delta-only files.

## Scope

- Market types: `Perpetual`, `Futures`, and `Option`.
- Spot: 项目未实现 Spot。Delta 官方用户手册列出现货交易，但当前 adapter 只声明 derivatives 产品线。
- Public REST: products/symbol rules, L2 order-book snapshots, public fee-rate
  extraction from product metadata.
- Private REST: balances, margined positions, place/cancel/query order, open
  orders, recent fills, cancel-all, and composed batch place/cancel.
- Adapter-specific helpers: `get_delta_option_chain` and
  `get_delta_funding_rates`.
- WebSocket: subscription/auth payload descriptors only. Public descriptors use
  `wss://public-socket.india.delta.exchange`; private descriptors use
  `wss://socket.india.delta.exchange`. Full typed WS parser integration remains
  future work.

## Endpoint Notes

The implementation follows Delta v2 docs for:

- `GET /v2/products`
- `GET /v2/l2orderbook/{symbol}`
- `POST /v2/orders`
- `DELETE /v2/orders`
- `GET /v2/orders/{order_id}`
- `GET /v2/orders/client_order_id/{client_order_id}`
- `GET /v2/positions/margined`
- `GET /v2/wallet/balances`
- `GET /v2/fills`
- `GET /v2/tickers`

REST authentication signs:

```text
METHOD + timestamp_seconds + request_path + body
```

Private WebSocket auth signs:

```text
GET + timestamp_seconds + /live
```

## Option Boundary

The shared gateway API does not yet expose a complete option contract, greeks,
volatility surface, or settlement model. Delta option products are represented
as `MarketType::Option` only where the common trading surface is sufficient.
Chain and greeks data stay in Delta-specific structs:

- `DeltaOptionContract`
- `DeltaGreeksSnapshot`
- `DeltaOptionChainSnapshot`

This avoids mapping strike, expiry, and greeks into unrelated spot/perp fields.

## Funding Boundary

There is no standard gateway funding-rate method yet. Delta funding is exposed
through `get_delta_funding_rates`, backed by ticker fields when present. The
helper returns `DeltaFundingRate` snapshots and should be migrated to a common
funding model when one exists.

## Fixtures

Delta fixtures live under `tests/fixtures/exchanges/delta/` and cover:

- Products and empty products.
- L2 order book.
- Option chain and greeks.
- Balances, orders, open orders, fills, positions.
- Error classification.
- REST HMAC signing vector.
- Request-spec examples for balances, place order, and cancel order.
