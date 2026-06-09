# Poloniex Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + USDT perpetual REST and WebSocket-spec adapter.

## Scope

- Adapter id: `poloniex`
- Default REST base URL: `https://api.poloniex.com`
- Config env: `POLONIEX_API_KEY`, `POLONIEX_API_SECRET`, `POLONIEX_RECV_WINDOW_MS`, `POLONIEX_PRIVATE_REST_ENABLED`, `POLONIEX_SPOT_PUBLIC_WS_URL`, `POLONIEX_SPOT_PRIVATE_WS_URL`, `POLONIEX_FUTURES_PUBLIC_WS_URL`, `POLONIEX_FUTURES_PRIVATE_WS_URL`
- Private REST signing: Poloniex V2 HMAC-SHA256 base64 over `METHOD\npath\nsorted params`, with `signTimestamp` and optional `requestBody`.
- WebSocket auth signing: Poloniex V2 HMAC-SHA256 base64 over `GET\n/ws\nsignTimestamp=...`.

## Endpoint Mapping

| Standard capability | Poloniex endpoint | Status |
| --- | --- | --- |
| Spot symbol rules | `GET /markets` | Implemented |
| Perp symbol rules | `GET /v3/market/allInstruments` | Implemented |
| Spot order book | `GET /markets/{symbol}/orderBook` | Implemented |
| Perp order book | `GET /v3/market/orderBook` | Implemented |
| Spot balances | `GET /accounts/balances?accountType=SPOT` | Implemented |
| Perp balances | `GET /v3/account/balance` | Implemented |
| Perp positions | `GET /v3/trade/position/opens` | Implemented |
| Spot fees | `GET /feeinfo` | Implemented |
| Spot orders | `POST /orders`, `DELETE /orders/{id}`, `DELETE /orders`, `GET /orders/{id}`, `GET /orders` | Implemented |
| Spot amend | `PUT /orders/{id}`, `PUT /orders/cid:{clientOrderId}` | Implemented for quantity/client-id amend |
| Spot batch orders | `POST /orders/batch`, `DELETE /orders/cancelByIds` | Implemented |
| Perp orders | `POST /v3/trade/order`, `DELETE /v3/trade/order`, `DELETE /v3/trade/allOrders`, `GET /v3/trade/order/details`, `GET /v3/trade/order/opens` | Implemented |
| Perp batch orders | `POST /v3/trade/orders`, `DELETE /v3/trade/batchOrders` | Implemented |
| Fills | `GET /trades`, `GET /v3/trade/order/trades` | Implemented |
| Spot WebSocket | `wss://ws.poloniex.com/ws/public`, `wss://ws.poloniex.com/ws/private` | Subscription specs plus book/trade/ticker/candle/order/balance parser coverage |
| Futures WebSocket V3 | `wss://ws.poloniex.com/ws/v3/public`, `wss://ws.poloniex.com/ws/v3/private` | Subscription specs plus book/trade/ticker/candle/order/fill/balance/position parser coverage |
| Futures amend/order lists | no verified Binance-compatible gateway mapping | Explicit `Unsupported` |

## Public Order Book WebSocket

| Product | URL | Channel | Depth | Cadence | Sequence | Checksum | Rebuild |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Spot | `wss://ws.poloniex.com/ws/public` | `book_lv2` | 20 levels | Real-time; no fixed ms published | `id` with previous `lastId` | Not documented | REST `GET /markets/{symbol}/orderBook`, then resubscribe |
| Futures V3 | `wss://ws.poloniex.com/ws/v3/public` | `book_lv2` | 20 levels | Real-time; no fixed ms published | `id` with previous `lid` | Not documented | REST `GET /v3/market/orderBook`, then resubscribe |

The adapter now exposes a structured `book_lv2` policy helper, parses `id` into
`OrderBookSnapshot.sequence`, and validates continuity with the documented
previous-id fields. A gap, regression, reconnect, stale stream, or parse failure
requires discarding the local book, rebuilding from REST, and resubscribing to
`book_lv2`.

## Notes

Symbols normalize to Poloniex underscore form: `BTC_USDT` for Spot and `BTC_USDT_PERP` for perpetual. Capability flags are coarse across Spot and perpetual: amend is enabled because Spot has a native cancel-replace endpoint, while perpetual amend still returns `Unsupported`. Stream methods currently return subscription specs/identifiers and parser coverage; production deployment still needs the shared WebSocket supervisor to connect, reconnect, heartbeat, and reconcile. Live trading should remain disabled until dry-run and live-readonly checks validate credentials, account mode, and position-side semantics.

Official public `book_lv2` is a 20-level real-time order book channel for both Spot and Futures V3. Spot messages use `id/lastId`; Futures V3 messages use `id/lid`. If the previous id does not line up with the next message, the local book must be discarded and the channel resubscribed. The reviewed official docs do not publish a fixed millisecond interval or checksum.

The machine-readable endpoint mapping lives at `crates/rustcta-exchange-gateway/src/adapters/poloniex/endpoint_mapping.yaml`. `capabilities_v2` declares StreamRuntime heartbeat/reconnect/resync behavior, relogin on private WS reconnect, native partial batch atomicity with a conservative shared max of 10 items across Spot and Futures, and REST reconciliation through query/open-orders/recent-fills after ambiguous mutations.
