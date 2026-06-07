# Bitrue Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + USDT-M perpetual REST adapter migrated for toolchain task 16 with adapter-local endpoint mapping, capabilities v2, fixtures, and WebSocket request/parser/session specs.

## Scope

- Adapter id: `bitrue`
- Spot REST: `https://openapi.bitrue.com`
- Futures REST: `https://fapi.bitrue.com`
- Spot public WebSocket: `wss://ws.bitrue.com/market/ws`
- Spot private WebSocket: `wss://wsapi.bitrue.com/stream?listenKey=<listenKey>`
- Futures private WebSocket: `wss://fapiws.bitrue.com/stream?streams=<listenKey>`
- Market types: `Spot`, `Perpetual`
- WebSocket runtime: session helper covers initial subscribe requests, ping/pong heartbeat response, reconnect state decisions and text-message parsers; the shared supervisor loop is still a platform integration step.
- Sandbox: no stable official sandbox URL confirmed; base URLs are configurable

## Endpoint Mapping

Machine-readable mapping: `crates/rustcta-exchange-gateway/src/adapters/bitrue/endpoint_mapping.yaml`.

| Gateway capability | Bitrue endpoint | Notes |
| --- | --- | --- |
| Spot symbol rules | `GET /api/v1/exchangeInfo` | Parses filters, precision, base/quote, min quantity and notional. |
| Perp symbol rules | `GET /fapi/v1/contracts` | Maps `E-BTC-USDT` contracts to `MarketType::Perpetual`. |
| Spot order book | `GET /api/v1/depth` | Uses compact Spot symbols such as `BTCUSDT`. |
| Perp order book | `GET /fapi/v1/depth` | Uses futures contract names such as `E-BTC-USDT`. |
| Spot balances and fee fallback | `GET /api/v1/account` | Parses balances plus account-level maker/taker commission fields, converted from integer bps-like values to rates. |
| Futures balances and positions | `GET /fapi/v2/account` | Parses account balances and nested `positionVos[].positions[]`. |
| Futures fees | `GET /fapi/v2/commissionRate` | Parses open/close maker/taker fields. |
| Spot order lifecycle | `POST/DELETE/GET /api/v1/order`, `GET /api/v1/openOrders` | Limit/market with base `quantity`, client order id, cancel, query, open orders. |
| Futures order lifecycle | `POST /fapi/v2/order`, `POST /fapi/v2/cancel`, `GET /fapi/v2/order`, `GET /fapi/v2/openOrders` | Limit/market/IOC/FOK/post-only, reduce-only mapped to `open=CLOSE`. |
| Batch place/cancel | composed gateway flow | Bitrue docs do not expose verified native batch endpoints; adapter executes validated single-order calls sequentially. |
| Cancel all | composed gateway flow | Queries open orders for one symbol, then cancels each order by id/client id. |
| Fills | `GET /api/v2/myTrades`, `GET /fapi/v2/myTrades` | Parses fee, maker/taker, trade id and timestamps; Spot side falls back to `isBuyer` when `side` is absent. |
| Spot public WebSocket | `wss://ws.bitrue.com/market/ws` | Official `event=sub` channels for `market_${symbol}_simple_depth_step0`, trades, ticker and kline; parser handles official `ping` and `tick.buys/sells` depth pushes. |
| Spot private WebSocket | `POST/PUT/DELETE /poseidon/api/v1/listenKey`, `/stream?listenKey=` | Subscription payloads for `user_order_update` and `user_balance_update`; parser handles subscribe ack, ping/pong, order/fill/balance updates. |
| Futures private WebSocket | `POST/PUT/DELETE /user_stream/api/v1/listenKey`, `/stream?streams=` | Subscription payloads for `user_order_update` and `user_account_update`; parser handles official `ORDER_TRADE_UPDATE` and `ACCOUNT_UPDATE` payloads. |
| Futures public WebSocket | configurable `futures_private_ws_url`/best-effort channel spec | Public futures WS channel names were not found in the official USDT-M GitHub docs; current public futures subscription id remains best-effort and must be live-dry-run validated. |

## Capabilities V2

- Public REST: native for Spot and USDT-M perpetual symbol rules and order book snapshots.
- Private REST: native when `BITRUE_API_KEY`, `BITRUE_API_SECRET`, and `BITRUE_PRIVATE_REST_ENABLED=true` are present.
- Private credentials scope: read-only and trade only; no withdraw/transfer scope is required or declared.
- Batch place/cancel: `ComposedSequential`, non-atomic, max 10 items, client order id supported, partial failure not supported. The adapter rejects larger batches before REST.
- Cancel all: composed by `get_open_orders` plus sequential `cancel_order`; symbol is required.
- History: order query/open-orders have no cursor pagination; fills support `startTime`, `endTime`, `limit`, max 1000.
- Unsupported conservatively: quote-sized market order, amend, OCO/order-list, native batch, stop orders, and Spot IOC/FOK/post-only until verified.

## Verification

- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway bitrue --lib --message-format short` passed 21 Bitrue tests with existing workspace warnings.

## Authentication

Spot uses `X-MBX-APIKEY` and a `signature` query parameter:

```text
HMAC_SHA256(secret, sorted_query_with_timestamp_and_recvWindow)
```

Futures uses JSON requests with `X-CH-APIKEY`, `X-CH-SIGN`, and `X-CH-TS`:

```text
HMAC_SHA256(secret, timestamp + METHOD + requestPathWithQuery + body)
```

Secrets are only used inside the transport signing path and are not written into request paths, query fields other than the signature, or test assertions.

## Error, Rate Limit, and Reconciliation

- Error classification maps Bitrue/Binance-like codes and HTTP status to authentication, permission, rate-limited, exchange-unavailable, invalid request, invalid symbol, order-not-found, insufficient-balance, duplicate client order id, and unknown classes.
- Rate limits are declared conservatively in `endpoint_mapping.yaml` as separate public/signed/order buckets for Spot and futures. No shared rate-limit executor is adapter-local here.
- REST reconciliation fallback is required for private state: use `query_order`, `get_open_orders`, and `get_recent_fills`; unknown orders should be queried by client order id or exchange order id before any replay decision.
- Kill-switch, disabled-symbol, and max-notional enforcement remain shared preflight/runtime responsibilities, not Bitrue adapter-local state.

## WebSocket Runtime Declaration

- Public WS: adapter-local subscription payloads for Spot trades, ticker, depth, and candles; parser handles ping/pong, depth, and trade messages. Futures public channel names remain best-effort pending live validation.
- Private WS: Spot listen key uses API-key requests to `/poseidon/api/v1/listenKey`; futures listen key uses signed requests to `/user_stream/api/v1/listenKey`.
- Heartbeat: server/application ping with echo pong; stale messages require reconnect and REST resync.
- Auth renewal: listen-key keepalive, renew before expiry, reconnect and resubscribe on renewal failure.
- Reconciliation after reconnect/auth renewal: resubscribe and resnapshot order books; refresh balances, positions, orders, and fills through REST.

## Fixtures

Fixtures live under `tests/fixtures/exchanges/bitrue/` and cover:

- Parser success: symbol rules, order book, balance, position, fees, order ack, open orders, fills.
- Empty response: `open_orders_empty.json`.
- Error response: `error.json` maps order-not-found classification.
- Missing critical field: `missing_symbol.json`.
- Signing/request coverage: tests assert Spot query signature headers/params and futures `X-CH-*` signing vector.

## Explicit Boundaries

- Quote-sized Spot market orders: Bitrue Spot docs require `quantity` for market orders.
- Spot post-only/IOC/FOK and stop orders are not enabled without a verified official equivalent.
- Native batch order endpoints, amend order, OCO/order-list APIs.
- Leverage, margin mode, position mode mutations.
- Full shared WebSocket supervisor orchestration and futures public WebSocket live validation.

Use REST reconciliation fallback as a safety net for private state until the shared WebSocket supervisor loop is wired and live-dry-run validated.
