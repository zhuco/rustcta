# HashKey Global Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + futures REST adapter with WebSocket request/parser/session specs.

## Scope

- Adapter id: `hashkey_global`
- Spot REST: `https://api-glb.hashkey.com`
- Futures REST: `https://api-glb.hashkey.com`
- Spot public WebSocket: `wss://stream-glb.hashkey.com/quote/ws/v1`
- Spot/private account WebSocket: `wss://stream-glb.hashkey.com/api/v1/ws`
- Market types: `Spot`, `Perpetual`
- WebSocket runtime: session helpers cover initial subscribe requests, ping/pong heartbeat responses, reconnect state decisions and text-message parsers. The shared supervisor loop remains a platform integration step.
- Sandbox: no stable official sandbox URL confirmed; base URLs are configurable.

## Endpoint Mapping

| Gateway capability | HashKey Global endpoint | Notes |
| --- | --- | --- |
| Spot symbol rules | `GET /api/v1/exchangeInfo` | Parses Binance-style `symbols[]` filters, base/quote, tick size, step size, min quantity and min notional. |
| Futures symbol rules | `GET /api/v1/futures/exchangeInfo` | Parses compact futures symbols such as `BTCUSDT` into `MarketType::Perpetual`. |
| Spot order book | `GET /api/v1/depth` | Uses compact Spot symbols such as `BTCUSDT`. |
| Futures order book | `GET /api/v1/futures/depth` | Uses compact futures symbols such as `BTCUSDT`. |
| Balances | `GET /api/v1/account`, `GET /api/v1/futures/account` | Parses Spot balances and futures account balances by requested market type. |
| Positions | `GET /api/v1/futures/account` | Parses futures position arrays with quantity, entry/mark/liquidation prices, PnL, leverage and side. |
| Fee rate | `GET /api/v1/account`, `GET /api/v1/futures/commissionRate` | Parses Spot account commission fallback and futures maker/taker commission fields. |
| Spot order lifecycle | `POST/DELETE/GET /api/v1/order`, `GET /api/v1/openOrders` | Limit/market with base `quantity`, client order id, cancel, query and open orders. |
| Futures order lifecycle | `POST/DELETE/GET /api/v1/futures/order`, `GET /api/v1/futures/openOrders` | Limit/market/post-only/IOC/FOK, client order id, `reduceOnly`, `positionSide`, cancel, query and open orders. |
| Batch place/cancel | composed gateway flow | Validates each command and executes single-order calls sequentially, returning unified batch responses. |
| Cancel all | composed gateway flow | Queries open orders for one symbol, then cancels each order by exchange id/client id. |
| Fills | `GET /api/v2/myTrades`, `GET /api/v1/futures/myTrades` | Parses fee, maker/taker, trade id, quantity/quote quantity and timestamps. |
| Public WebSocket | `wss://stream-glb.hashkey.com/quote/ws/v1` | Subscription specs and parsers cover order book, trades, ticker, candles, ack and ping/pong heartbeat. |
| Private WebSocket | `POST/PUT/DELETE /api/v1/userDataStream`, `wss://stream-glb.hashkey.com/api/v1/ws` | Listen-key URL helpers, private subscription specs and parsers cover order, fill, balance/account and position events. |

## Authentication

Signed REST requests use `X-HK-APIKEY` plus `timestamp`, `recvWindow` and `signature` query parameters:

```text
HMAC_SHA256(secret, sorted_query_with_timestamp_and_recvWindow)
```

Secrets are only used inside the transport signing path and are not written into request paths, non-signature query fields or test assertions.

## Explicit Boundaries

- Quote-sized Spot market orders are not enabled until HashKey account behavior is live validated.
- Native batch endpoints, amend order, OCO/OTO order-list APIs, leverage, margin mode and position mode mutations are outside the current shared gateway trait or remain explicit follow-ups.
- Batch place/cancel and cancel-all are composed gateway flows, not atomic native exchange batch requests.
- Production WebSocket socket supervision and live-dry-run reconciliation still need deployment validation before relying on WS-only private state.

Use REST reconciliation as the source of truth until API keys, permissions, minimum notional rules and live WebSocket behavior have been validated with a read-only preflight followed by live-dry-run.

## Task 22 Toolchain Status

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/hashkey_global/endpoint_mapping.yaml`.
- Capabilities v2: `toolchain.rs` declares Spot/perpetual public REST, gated private REST, REST-fallback WS runtime, listen-key renewal policy, composed batch place/cancel, REST reconciliation, credential scopes and 1000-item history limits.
- Fixtures: `tests/fixtures/exchanges/hashkey_global/` covers success, empty response, error response and missing required fields; public parser tests read fixture files directly.
- Request-spec/signing: private tests assert signed request paths/query/signature behavior; `hashkey_global_signing_should_match_known_hmac` covers the HMAC vector.
- WS policy: public/private WS are spec/parser ready with ping/pong heartbeat; private WS uses listen-key renewal and REST open-orders/account/fill reconciliation.
- Rate-limit/pagination/reconciliation/batch: endpoint mapping declares buckets, limit pagination and REST reconciliation. Batch is gateway-composed and partial/non-atomic, not native atomic batch.
- Live boundary: quote-sized Spot market, native batch, amend, order-list and position/margin mode mutations remain outside the current runtime.
- Validation: `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/hashkey-task-check cargo test -p rustcta-exchange-gateway hashkey_global --lib --message-format short` passed 16 HashKey Global tests with 736 filtered out and existing workspace warnings.
