# LBank Gateway Adapter

`crates/rustcta-exchange-gateway/src/adapters/lbank/` implements the industrial
gateway `ExchangeClient` adapter for LBank Spot and USDT perpetual public REST,
Spot private REST, Spot stream subscription specs, and the official-confirmed
subset of LBank perpetual private REST.

## Scope

- Spot public REST: symbol rules, order book snapshots, server time, currency
  pairs, ticker, book ticker, public trades, and klines.
- Spot private REST: balances, fee rate, place order, quote-sized buy market
  order, native batch place order, cancel order, composed batch cancel order,
  symbol-scoped cancel-all, query order, open orders, and recent fills/order
  transaction detail.
- Spot WebSocket specs/session helpers: public depth/trade/ticker/candle
  subscription payloads, private `orderUpdate`/`assetUpdate` subscribe-key
  payloads, public depth `OrderBookSnapshot` conversion, private order
  `OrderUpdate` conversion, private balance `BalanceSnapshot` conversion,
  ping/pong heartbeat helpers, heartbeat stream events, and Spot subscribe-key
  create/refresh/destroy REST helpers.
- Perpetual public REST: instruments and order book snapshots.
- Perpetual private REST: account balance readback, limit order placement, and
  gateway-composed batch limit order placement through the confirmed
  `placeOrder` endpoint. Positions are parsed from the official account
  response if LBank returns a `positions`/`positionList` array.
- Perpetual private extension: `post_contract_signed_raw` signs JSON POST
  requests for manually confirmed `/cfd/openApi/v1/prv/...` paths. This is a
  guarded escape hatch for LBank contract endpoints whose stable request tables
  are not present in the public docs; it does not mark those operations as
  standard supported gateway capabilities.
- REST signing: LBank MD5 prehash followed by HMAC-SHA256, using
  `api_key`, `timestamp`, `signature_method=HmacSHA256`, and `echostr`.

Machine-readable endpoint and capability metadata lives at
`crates/rustcta-exchange-gateway/src/adapters/lbank/endpoint_mapping.yaml`.
The runtime `capabilities()` response populates `capabilities_v2` with
Spot/perpetual batch mode, non-atomic composed cancel semantics, history
pagination, listen-key renewal policy, and credential scopes while preserving
legacy boolean compatibility.

## Endpoint Mapping

Machine-readable adapter-local declarations live in:

- `crates/rustcta-exchange-gateway/src/adapters/lbank/endpoint_mapping.yaml`
- `crates/rustcta-exchange-gateway/src/adapters/lbank/capabilities_v2.yaml`
- `tests/fixtures/exchanges/lbank/request_specs/*.json`
- `tests/fixtures/exchanges/lbank/signing_vectors/*.json`

| Standard capability | LBank endpoint |
| --- | --- |
| Spot `get_symbol_rules` | `GET /v2/accuracy.do` |
| Spot `get_order_book` | `GET /v2/depth.do` |
| LBank-specific `get_spot_server_time` | `GET /v2/timestamp.do` |
| LBank-specific `get_spot_currency_pairs` | `GET /v2/currencyPairs.do` |
| LBank-specific `get_spot_ticker` | `GET /v2/ticker.do` |
| LBank-specific `get_spot_book_ticker` | `GET /v2/supplement/ticker/bookTicker.do` |
| LBank-specific `get_spot_public_trades` | `GET /v2/trades.do` |
| LBank-specific `get_spot_klines` | `GET /v2/kline.do` |
| Spot `get_balances` | `POST /v2/supplement/user_info_account.do` |
| Spot `get_fees` | `POST /v2/supplement/customer_trade_fee.do` |
| Spot `place_order` | `POST /v2/supplement/create_order.do` |
| Spot `place_quote_market_order` | `POST /v2/supplement/create_order.do` with `type=buy_market` and quote amount in `price` |
| Spot `batch_place_orders` | `POST /v2/batch_create_order.do` |
| Spot `cancel_order` | `POST /v2/supplement/cancel_order.do` |
| Spot `batch_cancel_orders` | Gateway-composed calls to `POST /v2/supplement/cancel_order.do` |
| Spot `cancel_all_orders` | `POST /v2/supplement/cancel_order_by_symbol.do` |
| Spot `query_order` | `POST /v2/supplement/orders_info.do` |
| Spot `get_open_orders` | `POST /v2/supplement/orders_info_no_deal.do` |
| Spot `get_recent_fills` by order id | `POST /v2/order_transaction_detail.do` |
| Spot `get_recent_fills` without order id | `POST /v2/supplement/transaction_history.do` |
| Spot private stream key | `POST /v2/subscribe/get_key.do` |
| Spot private stream key refresh | `POST /v2/subscribe/refresh_key.do` |
| Spot private stream key destroy | `POST /v2/subscribe/destroy_key.do` |
| Spot public streams | `wss://www.lbkex.net/ws/V2/` `depth`, `trade`, `tick`, `kbar` subscriptions; depth pushes emit standard `OrderBookSnapshot` events |
| Spot private streams | `wss://www.lbkex.net/ws/V2/` `orderUpdate`, `assetUpdate` subscriptions; order updates emit `OrderUpdate`, balance updates emit `BalanceSnapshot` |
| Spot stream heartbeat | `{"action":"ping","ping":"..."}` / `{"action":"pong","pong":"..."}` |
| Perp `get_symbol_rules` | `GET /cfd/openApi/v1/pub/instrument` |
| Perp `get_order_book` | `GET /cfd/openApi/v1/pub/marketOrder` |
| Perp `get_balances` | `POST /cfd/openApi/v1/prv/account` |
| Perp `get_positions` | `POST /cfd/openApi/v1/prv/account`, best-effort parse when positions are included |
| Perp `place_order` | `POST /cfd/openApi/v1/prv/placeOrder` |
| Perp `batch_place_orders` | Gateway-composed calls to `POST /cfd/openApi/v1/prv/placeOrder` |
| LBank-specific contract raw POST | Guarded `POST /cfd/openApi/v1/prv/...` via `post_contract_signed_raw` |

## Capability Declaration

The adapter keeps the legacy `ExchangeClientCapabilities` bools compatible and
adds an adapter-local `capabilities_v2.yaml` declaration until the shared
Capability v2 structs land. Current support is:

- Spot: native public/private REST when credentials are present; public Spot WS
  request/session helpers; private Spot WS subscribe-key helpers.
- Perpetual: native public REST for instruments/order book; confirmed private
  account readback and limit `placeOrder`; positions are a REST fallback parsed
  from the account payload if present.
- Batch: Spot batch place is native, partial; Spot batch cancel is composed,
  non-atomic; perpetual batch place is composed through confirmed `placeOrder`;
  perpetual batch cancel is unsupported.
- Pagination: Spot open orders currently fetches page `1` with `page_length=100`.
  Spot fill history supports `fromId`, `startTim`, `endTime`, and `limit` with a
  max limit of `100`. Automatic multi-page sweep is deferred to shared
  pagination tooling.
- Reconciliation: after ambiguous Spot writes, query by exchange/client order id
  and then fetch symbol open orders. After Spot stream gaps, refetch REST
  snapshots and reconcile open orders, balances, and recent fills. Perpetual
  ambiguous lifecycle writes require manual review unless a desk has confirmed a
  raw endpoint and chosen to use `post_contract_signed_raw`.

## WebSocket Runtime Policy

- Public Spot WS supports `depth`, `trade`, `tick`, and `kbar` subscription
  payloads and heartbeat ping/pong helpers. Depth messages are normalized into
  standard `ExchangeStreamEvent::OrderBookSnapshot`; trade/ticker/kbar remain
  typed parser scope because the shared stream model lacks those event variants.
- Private Spot WS uses REST subscribe-key lifecycle:
  `get_key` -> subscribe -> periodic `refresh_key` -> `destroy_key` on shutdown.
  `orderUpdate` and `assetUpdate` messages convert into standard order and
  balance stream events.
- Heartbeat policy is bidirectional ping/pong. The adapter-local declaration uses
  a conservative 30 second local ping interval and 90 second stale timeout for
  future supervisor integration.
- Auth renewal policy refreshes the subscribe key before expiry; on refresh
  failure the supervisor should reconnect and recreate the key, then reconcile
  balances/open orders/fills.
- Perpetual public/private WS is not declared supported until payload, auth,
  heartbeat, and event parser semantics are confirmed.

## Rate Limit Plan

The adapter-local plan is conservative until shared rate-limit tooling consumes
venue headers:

- Public REST: `4 rps`, burst `6`, IP scoped.
- Private REST: `2 rps`, burst `4`, API-key scoped.
- Order writes: `1 rps`, burst `2`, API-key scoped.
- WS control messages: `1 rps`, burst `3`, connection scoped.

Mapped rate-limit codes include `10004`, `10012`, and `183`; auth, permission,
invalid-symbol, duplicate-client-id, insufficient-balance, min-notional, and
order-not-found classes are handled in `transport.rs`.

## Unsupported Boundaries

- Perpetual cancel/query/open orders/fills, leverage, margin mode, and position
  mode mutations are not wired because the official materials checked during
  this task did not expose stable endpoint specs for those operations.
- For desks that obtain LBank-side confirmation of a private contract path
  before this adapter gains a typed mapping, use `post_contract_signed_raw`.
  The helper validates the path prefix, applies the same contract JSON signing
  as `placeOrder`, and returns the raw parsed payload.
- Perpetual market orders and post-only orders are not declared supported in the
  gateway adapter; LBank's confirmed `placeOrder` mapping is currently limited
  to limit orders.
- The adapter now exposes offline-testable WebSocket session helpers for
  subscribe payloads, heartbeat ping/pong, runtime state, and subscribe-key
  lifecycle. A production live WebSocket supervisor still needs to own network
  connection, reconnect, and venue event parser dispatch end to end.
- Perpetual private WebSocket order/account channels are not declared supported
  until the production socket protocol and parser fixtures are confirmed.
- Spot batch cancel is gateway-composed and non-atomic; perpetual batch cancel is
  explicitly unsupported.

## Symbol Projection

- Spot native symbols use lowercase underscores, for example `btc_usdt`.
- Perpetual native symbols use uppercase concatenation, for example `BTCUSDT`.

## Credentials

Private REST requires:

```bash
export LBANK_API_KEY=...
export LBANK_API_SECRET=...
```

`RUSTCTA_LBANK_API_KEY`, `LBANK_SPOT_API_KEY`, `RUSTCTA_LBANK_API_SECRET`, and
`LBANK_SPOT_API_SECRET` are accepted as fallbacks by the gateway app. Keep
withdrawal permission disabled for trading keys.

## Validation

Targeted validation:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway lbank --lib --message-format short
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-exchange-gateway --lib --message-format short
```

The LBank tests cover public REST request routing, LBank-specific public market
helpers, parser behavior, signed Spot form requests, signed perpetual JSON
requests, official HMAC example signing, private operation gating, Spot stream
subscribe specs/session helpers, private subscribe-key create/refresh/destroy,
heartbeat ping/pong, public order-book stream conversion, private order/balance
stream conversion, order mutations, batch order requests, readbacks, fills,
balances, fees, perpetual account/order routes, and sanitized fixtures under
`tests/fixtures/exchanges/lbank/`. Latest targeted run passed 21 LBank tests
with existing workspace warnings; local mock REST tests require an unsandboxed
run when `127.0.0.1` binding is blocked.
