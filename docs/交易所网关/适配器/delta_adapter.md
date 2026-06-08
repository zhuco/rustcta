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

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。Delta 新公共 WS endpoint 支持 `ob_l1`、`ob_l2` 和 `ob_updates`；legacy `l2_orderbook`/`l2_updates` 已映射到新 channel。

`ob_l1` 是 L1，100ms；`ob_l2` 是 top 15 levels，500ms；`ob_updates` 首包 snapshot 后推全量 orderbook incremental update，100ms，有 `seq` 和 CRC32 `cs`。`seq` 必须 +1，不连续时 resubscribe 并从 snapshot 重建。

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
