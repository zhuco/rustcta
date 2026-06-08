# Backpack Gateway Adapter

Task 21 covers the `backpack` adapter for Backpack Exchange Spot and Perp.

## Official Surface

- REST base URL: `https://api.backpack.exchange`
- WebSocket URL: `wss://ws.backpack.exchange`
- Auth: Ed25519 signatures with `X-API-Key`, `X-Signature`, `X-Timestamp`, and `X-Window`
- REST signing payload: `instruction=<instruction>&...&timestamp=<ms>&window=<ms>`
- Native batch order signing repeats `instruction=orderExecute` for every order before appending timestamp/window.

## Endpoint Mapping

The machine-readable mapping is
`crates/rustcta-exchange-gateway/src/adapters/backpack/endpoint_mapping.yaml`.

| Standard capability | Backpack endpoint | Implementation |
| --- | --- | --- |
| Spot/perp symbol rules | `GET /api/v1/markets` | Parses `SPOT` and `PERP`, tick size, step size, min quantity/notional, visible markets only |
| Order book snapshot | `GET /api/v1/depth` | Parses bids/asks, update id, microsecond timestamp |
| Balances | `GET /api/v1/capital` (`balanceQuery`) | Parses asset `available`, `locked`, `staked` |
| Positions | `GET /api/v1/position` (`positionQuery`) | Perp positions with net quantity, entry, mark, liquidation, unrealized PnL |
| Fees | `GET /api/v1/account` (`accountQuery`) | Converts spot/futures maker/taker bps to decimal rates |
| Place order | `POST /api/v1/order` (`orderExecute`) | Market/limit/post-only/IOC/FOK, quote market, reduce-only for perp |
| Cancel order | `DELETE /api/v1/order` (`orderCancel`) | By order id or numeric client id |
| Native batch place | `POST /api/v1/orders` (`orderExecute`) | Uses Backpack batch signing canonicalization |
| Batch cancel | `DELETE /api/v1/order` (`orderCancel`) | Composed sequential cancels; Backpack has no documented native batch-cancel endpoint |
| Cancel all | `DELETE /api/v1/orders` (`orderCancelAll`) | Requires symbol |
| Query/open orders | `GET /api/v1/order`, `GET /api/v1/orders` | Signed query, market type filtering |
| Fills | `GET /wapi/v1/history/fills` (`fillHistoryQueryAll`) | Parses trade id, maker/taker, fee asset/amount |
| Public WS | `trade`, `ticker`, `depth`, `bookTicker`, `kline` | Subscribe payloads, heartbeat helpers, typed public trade/ticker/kline parsers, and depth/bookTicker standard order-book event conversion |
| Private WS | `account.orderUpdate`, `account.positionUpdate` | Signed subscribe payload with `instruction=subscribe`, stream parser helpers for order and position events |

## Capability Declaration

- Products: Spot and USDC perpetual.
- Public REST: symbol rules and order book snapshots.
- Private REST: balances, positions, fees, place order, quote market order,
  cancel order, cancel all, query order, open orders, recent fills, native batch
  place, composed batch cancel.
- Public WS runtime: native subscription specs for `trade`, `ticker`, `depth`,
  `bookTicker`, and `kline`; heartbeat uses client `PING` and expects `PONG` or
  heartbeat acknowledgement.
- Private WS runtime: native signed subscribe for `account.orderUpdate` and
  `account.positionUpdate`; private fills and balances are not declared until
  official stable channels are verified.

## Offline Verification Assets

- Parser fixtures:
  `tests/fixtures/exchanges/backpack/{markets,orderbook,balances,positions,account,order_ack,cancel_ack,cancel_all_ack,batch_order_ack,open_orders,fills,error}.json`.
- Request-spec fixtures:
  `tests/fixtures/exchanges/backpack/request_specs/*.json` cover all currently
  declared private REST operations plus public order-book limit normalization.
- Signing vectors:
  `tests/fixtures/exchanges/backpack/signing_vectors/order_cancel_ed25519.json`
  and
  `tests/fixtures/exchanges/backpack/signing_vectors/batch_order_execute_payload.json`.

## Validation

Targeted local validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/backpack/endpoint_mapping.yaml
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/backpack-task-check cargo test -p rustcta-exchange-gateway backpack --lib --message-format short
```

Latest targeted run passed 18 Backpack tests with 734 filtered out and existing
workspace warnings. Local mock REST tests require an unsandboxed run when
`127.0.0.1` binding is blocked.

## Rate Limits And Pagination

- Rate-limit plan: the endpoint mapping uses conservative public/private/order
  buckets because the adapter source does not contain verified official
  per-endpoint weights. The plan is intentionally auditable and should be
  tightened only after official limits are recorded.
- Pagination: order book uses `limit` capped at `1000`; recent fills support
  `from`, `to`, and `limit` capped at `1000`; symbol rules, balances,
  positions, fees, order lifecycle, open orders, and batch operations are
  non-paginated in the current adapter.

## Reconciliation And Batch Policy

- REST reconciliation before live trading should poll open orders, recent fills,
  positions, and balances after private stream disconnects, unknown order state,
  batch partial failure, or cancel/place timeout.
- Public order-book resync uses REST `GET /api/v1/depth` after reconnect,
  subscription loss, or sequence gap detection.
- Native batch place uses `POST /api/v1/orders`; the adapter does not claim
  atomicity and skips item-level error objects in parsed order states.
- Batch cancel is composed sequentially from `DELETE /api/v1/order`, so it is
  non-atomic and may partially succeed.

## Heartbeat And Auth Renewal

- Public/private WS heartbeat policy: send `{"method":"PING"}` every 30 seconds
  and reconnect/resubscribe if no pong/heartbeat is observed within 10 seconds.
- Private WS auth renewal policy: no listen-key lease exists in this adapter.
  Reconnect and send a fresh signed `SUBSCRIBE` payload before the signed
  timestamp window is stale; mapping records a 5 minute relogin cadence.

## Boundaries

- Backpack client order IDs are numeric `uint32`; non-numeric client IDs return `InvalidRequest`.
- OCO/OTO order lists and amend order are not documented as stable native Backpack APIs and return `Unsupported`.
- Batch cancel is intentionally composed from official single-cancel calls, so it is not atomic.
- Separate private fill, balance, and aggregate account WebSocket channels are not advertised until an official channel name and payload schema are verified. Fills and balances remain covered by REST reconciliation.
- Public `depth` is treated as a best-effort delta stream until a runtime
  consumer pairs it with a REST snapshot, sequence-gap detection, and resync.
  The shared `ExchangeStreamEvent` currently has no public trade/ticker/candle
  variants, so those messages are exposed through typed Backpack parser helpers
  while order-book updates map to standard order-book events.
