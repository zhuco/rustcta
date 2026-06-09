# Coinstore Gateway Adapter

Status date: 2026-06-08

Status: `rustcta-exchange-gateway` Spot + futures REST adapter with public/private WebSocket request specs and task-24 toolchain metadata.

## Scope

- Adapter id: `coinstore`
- Spot REST: `https://api.coinstore.com/api`
- Futures REST: `https://futures.coinstore.com/api`
- Spot public WebSocket: `wss://ws.coinstore.com/s/ws`
- Futures WebSocket: `wss://ws-futures.coinstore.com/socket.io/?EIO=3&transport=websocket`
- Market types: `Spot`, `Perpetual`
- Sandbox: no stable official sandbox URL confirmed; base URLs are configurable.

## Endpoint Mapping

Machine-readable mapping: `crates/rustcta-exchange-gateway/src/adapters/coinstore/endpoint_mapping.yaml`.

| Gateway capability | Coinstore endpoint | Notes |
| --- | --- | --- |
| Spot symbol rules / fees | `POST /v2/public/config/spot/symbols` | Parses base/quote, precision, min size/notional, maker/taker fees. |
| Futures symbol rules / fees | `GET /api/configs/public` | Parses `contracts[]`, tick/size limits, maker/taker rates and numeric `contractId` exchange symbols. |
| Spot order book | `GET /v1/market/depth/{symbol}` | Supports documented depths up to 100. |
| Futures order book | `GET /v1/futureQuot/querySnapshot` | Uses numeric `contractId`; parses snapshot bids/asks from futures quote payloads. |
| Balances | `POST /spot/accountList`, `POST /api/future/queryAvail` | Signed Spot and futures balance readback. |
| Positions | `GET /api/future/queryPosi` | Futures positions only. |
| Order lifecycle | `POST /trade/order/place`, `POST /trade/order/cancel`, `GET /api/v2/trade/order/orderInfo`, `GET /api/v2/trade/order/active` | Spot market/limit/post-only/IOC/FOK, client order id, cancel/query/open orders. |
| Quote market | `POST /trade/order/place` | Spot buy market orders use documented `amount` quote sizing; Spot sell and futures quote-market are explicit `Unsupported`. |
| Futures order lifecycle | `POST /api/trade/order/place`, `POST /api/trade/order/cancel`, `GET /api/v2/trade/order/orderInfo`, `GET /api/v2/trade/order/active` | Uses numeric `contractId`; limit/market with client order id; reduce/close is mapped through `positionEffect=2`. |
| Native batch place | `POST /trade/order/placeBatch`, `POST /api/trade/order/placeBatch` | Spot and futures batch order submit. |
| Native batch cancel | `POST /trade/order/cancelBatch`, `POST /trade/order/cancelBatchByClOrdId`, `POST /api/trade/orders/del` | Spot by order id or client id; futures native multi-order cancel. |
| Cancel all | `POST /api/trade/order/cancelAll` | Futures only; Spot cancel-all is explicit `Unsupported`. |
| Fills | `GET /trade/match/accountMatches`, `GET /api/v2/trade/order/queryHisMatch` | Parses recent fills, fees and timestamps. |
| Spot public WebSocket | `{"op":"SUB","channel":["<symbol>@depth@20"],"id":1}` | Supports trade, ticker, depth and kline channel specs plus pong helper; depth messages convert to standard `OrderBookSnapshot` stream events. |
| Futures public/private WebSocket | Socket.IO event text frames such as `42["subscribe", ...]` and `42["auth", ...]` | Supports public topics `future_tick`, `future_kline`, `future_snapshot_depth`, `indicator`; private `match` orders/fills standard event parsing. |

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。Coinstore Spot public WS 为 `wss://ws.coinstore.com/s/ws`，depth 订阅使用 `{"op":"SUB","channel":["btcusdt@depth"],"id":1}`；项目已有 `<symbol>@depth@20` fixture 可以作为 20 档变体继续核验。Futures Socket.IO public topic 包括 `future_snapshot_depth`，重建使用 REST snapshot。

Spot 官方写有变化才推，最小推送间隔 100ms。server message 有 session 级序号 `S`，可用于发现漏消息；官方未见 checksum。Spot/Futures 断线或序号异常时用 REST depth/snapshot 重建。

## Authentication

Signed REST requests use:

- `X-CS-APIKEY`
- `X-CS-EXPIRES` as a millisecond timestamp
- `X-CS-SIGN`

The signature is Coinstore's two-stage HMAC-SHA256:

```text
payload = query_string + raw_json_body
time_key = floor(expires_ms / 30000)
derived_key = hex(HMAC_SHA256(secret, time_key))
signature = hex(HMAC_SHA256(derived_key_as_utf8, payload))
```

Futures WebSocket auth uses the same signature with an empty payload.

## Capability V2

`CoinstoreGatewayAdapter::capabilities()` declares v2 support in addition to
legacy booleans:

- `public_rest`: native.
- `private_rest`: native only when `enabled_private_rest`, `api_key`, and
  `api_secret` are present; otherwise unsupported.
- `public_streams`: native for Spot WebSocket and futures Socket.IO.
- `private_streams`: native for futures Socket.IO `match` order/fill streams
  with credentials; Spot private stream, balance stream and position stream are
  unsupported with REST reconciliation fallback.
- `stream_runtime`: separate public/private connections, subscribe and
  unsubscribe payload helpers, reconnect with relogin and resubscribe, order
  book REST snapshot resync after reconnect/stale/gap, and private REST resync
  for orders, fills, balances and positions.
- `batch_place_orders` / `batch_cancel_orders`: native, partial atomicity,
  same market type required, client order ids supported, `max_items=20`.
- `amend_order` / `place_order_list`: explicit unsupported boundaries; the
  mapping has no lossless shared in-place amend or OCO/OTO/order-list semantics,
  so callers must cancel/replace or use venue-specific logic outside the shared
  gateway.
- `order_history`: private REST open-order/query-order reconciliation, no
  cursor; order id and client order id filters are supported.
- `fills_history`: private REST recent fills with `limit` up to 100 and
  order/client-order filters; no cursor.

## Request Specs, Signing Vectors and Fixtures

Offline coverage is adapter-local:

- Request-spec tests:
  `crates/rustcta-exchange-gateway/src/adapters/coinstore/private_tests.rs::coinstore_request_specs_should_match_public_and_private_http_shapes`.
- Signing vector tests:
  `coinstore_signing_vector_should_match_two_stage_hmac_fixture`, using
  legacy `tests/fixtures/exchanges/coinstore/signing_vectors.json` plus standard
  `tests/fixtures/exchanges/coinstore/signing_vectors/two_stage_hmac_query_plus_body.json`.
- Parser fixture tests:
  `coinstore_parser_fixtures_should_cover_success_empty_error_and_missing_fields`
  and stream fixture checks use `tests/fixtures/exchanges/coinstore/`.
- Fixture set covers successful Spot symbols, futures contracts, order book,
  balances, positions, order, open orders, fills, empty response, error
  response, missing critical field, public WS book and private WS match.

## Runtime Policy

Public streams:

- Spot WebSocket uses server ping / client JSON pong. The adapter documents
  180s server ping and 600s pong timeout in the subscription spec.
- Futures streams use Socket.IO text frames: ping `2`, pong `3`.
- Reconnect policy is 30s ping interval, 10s pong timeout, 180s stale-message
  threshold, 1s initial backoff, unlimited attempts.
- Order books are snapshot-only in normalized capability. Runtime resync uses
  REST snapshots after reconnect, stale stream, sequence gap or manual resync.

Private streams:

- Futures private Socket.IO emits `auth` then subscribes to `match`.
- The `match` parser emits standard `OrderUpdate` events for order
  subscriptions and `Fill` events for fill subscriptions.
- Balance and position pushes are not declared native; use REST readbacks after
  reconnect, auth renewal, stale stream or manual reconciliation.
- Auth renewal policy is relogin/resubscribe every 25s, renew-before-expiry
  30s, reconnect on renewal failure.
- Spot private WebSocket remains unsupported because the public docs do not
  provide a complete authenticated subscription contract.

## Rate Limit Plan

Coinstore response headers are not yet verified in this workspace. The adapter
declares a conservative local fixed-window plan in `endpoint_mapping.yaml`:

- `coinstore.rest.shared`: 60 unit-cost requests per second per exchange.
- `coinstore.private.trade`: 30 unit-cost private requests per second per
  account.
- All mapped endpoints use unit weight until live header behavior is measured.

This is intentionally conservative and should be tightened only after
read-only live probing confirms headers or published per-endpoint weights.

## Pagination and Reconciliation

- Public symbol, config, balance, position and order book endpoints are
  non-paginated in the adapter.
- Order book Spot depth uses `depth` with max 100.
- Recent fills support `limit` with max 100 plus symbol/order/client-order
  filters; cursor pagination is unsupported.
- Open orders support optional symbol filtering; cursor and limit pagination
  are unsupported.
- REST reconciliation source of truth:
  - `get_open_orders` after private WS reconnect, auth renewal, stale stream,
    unknown order ack or order timeout.
  - `query_order` by order id or client order id for targeted unknown-order
    resolution.
  - `get_recent_fills` for fill backfill after private stream gaps.
  - `get_balances` and `get_positions` after private stream reconnect or
    balance/position gap.

## Batch Capability

- Native Spot batch place: `POST /trade/order/placeBatch`.
- Native futures batch place: `POST /api/trade/order/placeBatch`.
- Native Spot batch cancel: `POST /trade/order/cancelBatch` or
  `POST /trade/order/cancelBatchByClOrdId`.
- Native futures batch cancel: `POST /api/trade/orders/del`.
- Same market type is required per request. Mixed Spot/perpetual batch requests
  are rejected before sending.
- Atomicity is declared partial because exchange payloads can include per-item
  results; callers should reconcile with open orders and recent fills after
  partial or ambiguous results.
- Adapter cap is `max_items=20` until live limits are verified.

## Explicit Boundaries

- Spot private WebSocket is not enabled because the official docs mention `LOGIN` but do not provide a complete authenticated subscription contract.
- Futures private balance/account/position stream subscriptions remain explicit `Unsupported`; balances and positions are recovered via REST reconciliation.
- Futures IOC/FOK/post-only and quote-sized market orders, Spot sell quote-market, Spot cancel-all, account-specific Spot fee endpoints, funding-history/open-interest endpoints, position-mode switching, OCO/OTO order lists, dead-man switch and amend order remain explicit `Unsupported` or outside the current shared trait.
- Futures Socket.IO ping/pong is handled as text frame specs; exact live cadence still needs live validation.
- Use REST reconciliation as source of truth until API keys, permissions, notional limits and live WebSocket behavior have been validated with read-only preflight and live-dry-run.

## Official Position Detail

仓位接口核验见 [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)。Coinstore Futures `GET /api/future/queryPosi` 已由当前项目 `get_positions` runtime 覆盖；balance/position stream 仍保持 unsupported，断线后用 REST reconciliation。

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinstore/endpoint_mapping.yaml
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/coinstore/*.rs
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway coinstore --lib --message-format short
```

The focused Coinstore tests cover request-specs, signing vectors, parser fixtures, public/private stream payloads, public book `OrderBookSnapshot` conversion, heartbeat classification, Socket.IO auth, private `match` order/fill standard event parsing, unsupported balance/position stream boundaries and runtime capability assertions. Latest targeted run passed 26 Coinstore tests. Local mock REST tests bind `127.0.0.1`; under the managed sandbox they require an unsandboxed test run.
