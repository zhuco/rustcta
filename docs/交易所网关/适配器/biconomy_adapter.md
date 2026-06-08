# Biconomy Exchange Gateway Adapter

Status date: 2026-06-08

Adapter id: `biconomy`

Implementation status: Spot REST and WebSocket request/parser/session specs are implemented behind `rustcta-exchange-api::ExchangeClient`. The adapter covers public symbol rules, order book snapshots, private balances, fee readback, order lifecycle, open orders, recent fills, gateway-composed batch place/cancel, symbol-scoped cancel-all, current public JSON-RPC WebSocket subscription payloads/parsers, private WebSocket order/balance subscription payloads and standard stream-event parsers, and ping/pong heartbeat helpers.

Futures/perpetual support is intentionally not declared yet. During task 23, the stable public Biconomy documentation surface available for implementation only supported the Spot-style REST/WS paths used here. Futures positions and non-Spot market requests return `Unsupported` instead of using unverified endpoints.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST + private REST + public/private WS specs |
| Futures/perpetual | n/a | Explicit `Unsupported` until official endpoint specs are confirmed |
| Testnet | n/a | Unsupported; no stable public sandbox host verified |

REST base URL: `https://www.biconomy.com`
Public WS: `wss://bei.biconomy.com/ws`
Private WS: `wss://www.biconomy.com/ws`

## Authentication

Private REST requests use:

- Header: `X-CH-APIKEY`
- Header: `X-CH-SIGN`
- Header: `X-CH-TS`
- Signature: hex `HMAC-SHA256(secret, timestamp + METHOD + path + body)`

The request-spec tests assert that signed private calls include the API key, timestamp and signature headers, and that secrets are not written into paths or JSON bodies.

## Endpoint Mapping

| Standard capability | Biconomy endpoint/spec | Current implementation |
| --- | --- | --- |
| Spot symbol rules | `GET /api/v1/exchangeInfo` | Parses `symbols[]`, base/quote assets, precision, tick size, step size, min quantity and min notional. |
| Spot order book | `GET /api/v1/depth` | Snapshot parser with `limit` clamped to 1..100. |
| Balances | `POST /api/v2/private/account` | Parses free/locked balances into the unified exchange balance model. |
| Fee rate | `POST /api/v2/private/account` | Parses account maker/taker commission fields and applies them to requested symbols. |
| Place order | `POST /api/v2/private/order` | Supports Spot market/limit/post-only/IOC/FOK request mapping and client order ids. |
| Cancel order | `POST /api/v2/private/cancel` | Cancels by exchange order id or client order id and normalizes sparse cancel acknowledgements to `Cancelled`. |
| Batch place/cancel | Gateway-composed flow | Sequentially executes single-order place/cancel calls and returns unified batch responses; not atomic native batch. |
| Cancel all | Gateway-composed flow | Requires a symbol, queries open orders, then cancels each order. |
| Query/open orders | `POST /api/v2/private/orderInfo`, `POST /api/v2/private/openOrders` | Parses unified order state, status, quantity, filled quantity and price. |
| Recent fills | `POST /api/v2/private/myTrades` | Parses trade id, order id, price, quantity, commission asset/amount and maker/taker flag. |
| Public WebSocket | `depth.subscribe`, `deals.subscribe`, `state.subscribe`, `kline.subscribe`; pushes `depth.update`, `deals.update`, `state.update`, `kline.update` | JSON-RPC subscription payload builders, ack/pong control parsing, order-book standard event conversion, and typed public trade/ticker/candle parsers. |
| Private WebSocket | `spot/user.order`, `spot/user.balance` | Subscription payload builders; private order updates emit `OrderUpdate` and balance pushes emit standard `BalanceSnapshot`; private position streams remain unsupported. |
| Heartbeat | `{"method":"server.ping","params":[],"id":...}` / `{"result":"pong"}` | Session helpers send official public heartbeat payloads, respond to legacy ping frames, and update runtime heartbeat state. |

## Validation

- `rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/biconomy/streams.rs crates/rustcta-exchange-gateway/src/adapters/biconomy/stream_tests.rs`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway biconomy --lib --message-format short`
  passed: 15 tests passed, 0 failed, 736 filtered out. The managed sandbox blocks local mock REST binding on `127.0.0.1`, so the full target run was rerun outside the sandbox.
- `CARGO_TARGET_DIR=target/biconomy-app-final-check2 cargo test -p rustcta-gateway config_should_parse_adapters_and_redirection_urls_without_secret_fields -- --nocapture`
  passed: 1 test passed, 0 failed.

Use Biconomy with private REST disabled until read-only account, fee and open-order calls have been verified with exchange-issued credentials.

## Task 20 Toolchain Status

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/biconomy/endpoint_mapping.yaml`.
- Capabilities v2: `toolchain.rs` declares Spot public/private REST, REST-fallback public/private WS, composed non-atomic batch place/cancel, REST reconciliation, credential scopes and history limits.
- Fixtures: `tests/fixtures/exchanges/biconomy/` covers symbol rules, order book, account, order ack and private WS order payloads; public parser tests read the fixture files directly.
- Request-spec/signing: private tests assert signed request path/header/body behavior and `signing.rs` has a deterministic HMAC vector.
- WS policy: public/private WS are spec/parser ready with heartbeat helpers; public books and private order/balance messages convert into standard stream events. REST snapshots/open orders remain resync source.
- Rate-limit/pagination/reconciliation/batch: declared in endpoint mapping and capabilities v2. Batch is gateway-composed with per-item partial failure; native atomic batch is not declared.
- Live boundary: keep private REST/WS behind credential gates and use REST reconciliation until live read-only and live-dry-run validation.
