# Phemex Gateway Adapter

Status date: 2026-06-07

Adapter id: `phemex`

Implementation status: Spot + USDT perpetual gateway support is implemented
behind `rustcta_exchange_api::ExchangeClient`. The standard surface covers
public REST, private REST, order lifecycle, composed batch place, perpetual
batch cancel, account readbacks, public/private WebSocket subscription specs,
heartbeat/parser behavior, and Phemex-specific account, position, transfer,
convert, deposit, and withdraw wrappers.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST + private REST + public/private WS specs |
| USDT perpetual | `Perpetual` | Public REST + private REST + public/private WS specs |
| Testnet | n/a | Supported through configurable `rest_base_url` / WS URLs |

Default REST base URL: `https://api.phemex.com`
Default public WS: `wss://ws.phemex.com`
Default private WS: `wss://ws.phemex.com`

## Authentication

REST private requests use:

- Header: `x-phemex-access-token`
- Header: `x-phemex-request-expiry`
- Header: `x-phemex-request-signature`
- Signature: lowercase hex `HMAC-SHA256(secret, path + query + expiry + body)`

Private WebSocket auth uses `user.auth` with signature over
`api_key + expiry`.

## Endpoint Mapping

| Standard capability | Phemex endpoint | Current implementation |
| --- | --- | --- |
| symbol rules | `GET /public/products-plus` | Spot and USDT perpetual parser with tick/step/notional limits |
| order book | `GET /md/orderbook` and `GET /md/v2/orderbook` | Snapshot parser, max depth 30 |
| balances | `GET /spot/wallets`, `GET /g-accounts/accountPositions` | Spot wallets and USDT account balances |
| positions | `GET /g-accounts/accountPositions` | USDT-M position parser |
| fees | `GET /api-data/spots/fee-rate`, `GET /api-data/futures/fee-rate` | Spot and USDT-M maker/taker rates |
| place order | `PUT /spot/orders/create`, `PUT /g-orders/create` | market/limit/post-only/IOC/FOK/GTX, client id, reduce-only |
| quote market order | `PUT /spot/orders/create` | Spot quote-sized market order with `qtyType=ByQuote` |
| amend order | `PUT /spot/orders`, `PUT /g-orders/replace` | Quantity amend by order id or client id |
| batch place | composed `PUT /spot/orders/create` / `PUT /g-orders/create` | Standard batch response from per-order signed requests |
| cancel order | `DELETE /spot/orders`, `DELETE /g-orders/cancel` | order id or client id |
| batch cancel | `DELETE /g-orders` | Perpetual native batch cancel for one symbol and one id type |
| cancel all | `DELETE /spot/orders/all`, `DELETE /g-orders/all` | Symbol-scoped cancel all |
| query order | `GET /api-data/spots/orders/by-order-id`, `GET /api-data/g-futures/orders/by-order-id` | order id or client id |
| open orders | `GET /spot/orders/activeList`, `GET /g-orders/activeList` | Symbol-scoped active orders |
| fills | `GET /api-data/spots/trades`, `GET /api-data/g-futures/trades` | fills, fee, liquidity, realized PnL parser |
| public WebSocket | `orderbook`, `trade`, `kline`, `market24h` methods | Spot and perpetual subscription payloads and parsers |
| private WebSocket | `wo.subscribe`, `aop_p.subscribe`, `ras_p.subscribe` | Spot wallet/order/fill, USDT-M account/order/position, risk account parser |

## Public WebSocket Order Book

Official `orderbook.subscribe` / `orderbook_p.subscribe` supports depth 0/1/5/10/30. The fast mode is about 20ms for requested depth, the aggregated mode is about 120ms, and full depth is 100ms; Phemex also publishes snapshot messages roughly every 60s for self-verification. Messages carry `sequence` and `type=snapshot/incremental`; no checksum is declared, so sequence continuity plus fresh REST/WS snapshot rebuild is required after gaps.

## Phemex-Specific Extensions

- Public REST helpers for server time, fullbook, index sources, ticker variants,
  funding history, perpetual candles, products-plus, chain settings, real
  funding rates, and trader performance.
- Private wrappers for funding fee history, closed positions, spot funds
  history, risk units, legacy wallet history, `/phemex-deposit/*`,
  `/phemex-withdraw/*`, `/assets/transfer`, spot/futures sub-account transfer,
  universal transfer, `/assets/quote`, and `/assets/convert`.
- Position controls: `switch_position_mode`, `set_one_way_leverage`,
  `set_hedged_leverage`, and `assign_position_balance`.
- Restricted raw signed GET/POST/POST JSON/PUT/DELETE helpers for official
  endpoints not yet modeled as strong typed gateway calls.

## Unsupported Boundaries

- Spot REST candles return `Unsupported` because the official Spot REST section
  does not expose a stable equivalent endpoint in the checked API surface.
- Dated futures are not declared by this adapter; only Spot and USDT/USDC
  perpetual rows from Phemex product metadata are mapped.
- Options are intentionally not modeled for Task 14.
- Standard order lists are not declared supported.
- Dead-man switch is not declared supported because no official Phemex endpoint
  was found for the Binance-equivalent behavior.
- Manual risk-limit mutation is not declared supported because the hedged
  contracts API marks manual risk-limit handling as deprecated.

## Task 14 Toolchain Declarations

Endpoint mapping lives at
`crates/rustcta-exchange-gateway/src/adapters/phemex/endpoint_mapping.yaml`.
The runtime capability declaration is also exposed through
`ExchangeClientCapabilities.capabilities_v2`.

| Requirement | Phemex declaration |
| --- | --- |
| capabilities v2 | Native public REST/WS; private REST/WS native only when credentials are configured |
| request-spec tests | Existing mocked REST tests assert signed methods, paths, query fields, and private operation gating |
| signing vector tests | REST `path + query + expiry + body` and WS `api_key + expiry` HMAC-SHA256 vectors |
| parser fixture tests | `tests/fixtures/exchanges/phemex/` covers success, empty response, error-shaped response, missing order book bids, order ACK, funding, and private WS order events |
| public WS runtime | Native subscribe specs for orderbook/trade/kline/ticker; client `server.ping` heartbeat |
| private WS runtime | `user.auth` plus `wo.subscribe`, `aop_p.subscribe`, and `ras_p.subscribe`; reconnect requires relogin and resubscribe |
| heartbeat policy | `server.ping` every 15s, 30s pong timeout, 45s stale-message threshold |
| auth renewal policy | Re-login before expiry; no listen key lease; reconnect and resubscribe on renewal failure |
| rate-limit plan | Endpoint mapping declares `public_market`, `private_account`, and `private_trade` buckets with retry-after handling from HTTP 429 or venue error messages |
| pagination | Orders/fills/funding history declare `limit` and cursor support; fills/funding also declare since/until support, max limit 200 |
| reconciliation | After unknown place/cancel or WS recovery, query order/open orders/recent fills and refresh balances/positions through REST |
| live-dry-run controls | Requires application kill-switch, disabled-symbol registry, and max-notional preflight because Phemex dead-man switch is not declared native |
| batch place | Composed sequential Spot/Perp single-order requests, non-atomic, partial failure visible |
| batch cancel | Native USDT perpetual `DELETE /g-orders`, same symbol and market type required, max 20, partial failure possible; Spot uses composed single cancels |
| contract spec | Perpetual contract metadata is sourced from `perpProductsV2`: base/quote/settle asset, tick size, quantity step, precision, and min notional |
| orderbook strictness | REST and stream book handling are `snapshot_only`; sequence is preserved, checksum is not declared, REST snapshot is required after reconnect/resync |
| future/perp boundary | `MarketType::Perpetual` only for listed USDT/USDC perpetual rows; no dated futures are opened |
| funding/open interest | Funding history and real funding-rate helpers are native; open interest is exposed from perpetual ticker `openInterestRv` |

## Credentials

Private REST requires:

```bash
export PHEMEX_API_KEY=...
export PHEMEX_API_SECRET=...
```

The gateway app also accepts `RUSTCTA_PHEMEX_API_KEY`,
`RUSTCTA_PHEMEX_API_SECRET`, `PHEMEX_SPOT_API_KEY`, and
`PHEMEX_SPOT_API_SECRET` fallbacks. Keep withdrawal permission disabled for
trading keys.

## Validation

Targeted validation:

```bash
cargo test -p rustcta-exchange-gateway phemex --all-features
```

The Phemex tests cover public REST routing/parsers, signed private REST,
private operation gating, place/amend/cancel/batch-place/batch-cancel/cancel-all
behavior, fees, balances, positions, fills, transfer/deposit/withdraw/convert
extensions, raw signed endpoints, public/private WebSocket subscription
payloads, auth payloads, heartbeat/control parsing, and private stream event
conversion.
