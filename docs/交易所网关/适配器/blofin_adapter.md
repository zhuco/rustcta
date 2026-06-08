# BloFin Gateway Adapter

`blofin` implements the RustCTA gateway `ExchangeClient` surface for BloFin
USDT perpetual contracts using the verified official OpenAPI.

Official docs checked on 2026-06-07:

- REST/WS guide: <https://github.com/blofin/blofin-api-docs/blob/main/index.md>
- REST base URL: `https://openapi.blofin.com`
- Public WS: `wss://openapi.blofin.com/ws/public`
- Private WS: `wss://openapi.blofin.com/ws/private`
- Demo REST/WS base: `https://demo-trading-openapi.blofin.com`

## Scope

Supported:

- Perpetual public REST: instruments, tickers, order book snapshots, trades,
  mark price, funding rate/current + history, regular/index/mark-price candles,
  and position tiers.
- Perpetual private REST: balances, positions, place/cancel order, native batch
  place, native batch cancel, cancel-all sweep, query/open orders, recent fills.
- BloFin-specific private REST helpers for asset balances, funds transfer,
  transfer/deposit/withdraw history, account config, currencies, demo-funds
  application, positions history, margin mode, position mode, leverage readback
  and mutation, TPSL orders, algo orders, close-position, completed order
  history, TPSL/algo history, and trade order price ranges.
- Perpetual public WS subscription specs for books, `books5`, trades, tickers,
  candles, and funding-rate.
- Perpetual private WS login + subscription specs for orders, positions, and
  account, plus a BloFin-specific `orders-algo` subscription payload helper.
- WS heartbeat spec: text `ping` every 20-30 seconds, expecting `pong`.
- REST and WS signing with `ACCESS-*` headers/login fields using BloFin's
  HMAC-SHA256 hex-then-Base64 signature.

Explicitly unsupported / not implemented:

- Spot trading is 项目未实现 Spot, not `交易所不支持现货`. BloFin official
  product/help pages document spot trading, while the verified OpenAPI used by
  this adapter is still dominated by `SWAP`/perpetual order lifecycle. A
  separate task must verify whether a stable Spot API order lifecycle is public.
- Unified `ExchangeClient` trait methods for quote-sized market orders, amend
  order, and OCO/OTO order lists. BloFin-specific TPSL/algo order methods are
  available outside the shared trait.
- A standalone private fills WS channel. Fills are available from REST
  `GET /api/v1/trade/fills-history`; private `orders` pushes can include fill
  state but are not advertised as an independent fills stream.
- Production WebSocket supervisor/runtime connection management remains a
  platform-level follow-up; this adapter currently exposes subscription/login
  specs and parsers.
- Dated futures and options. BloFin fixtures can include non-linear contract
  rows for boundary testing, but this adapter only declares `MarketType::Perpetual`.
  No options model is added or inferred.

## Environment

```bash
RUSTCTA_GATEWAY_ADAPTERS=blofin
RUSTCTA_BLOFIN_REST_BASE_URL=https://openapi.blofin.com
RUSTCTA_BLOFIN_API_KEY=...
RUSTCTA_BLOFIN_API_SECRET=...
RUSTCTA_BLOFIN_API_PASSPHRASE=...
```

Use `https://demo-trading-openapi.blofin.com` for demo trading accounts.

## Endpoint Mapping

The machine-readable mapping is
`crates/rustcta-exchange-gateway/src/adapters/blofin/endpoint_mapping.yaml`.

| Capability | BloFin endpoint/channel |
| --- | --- |
| symbol rules | `GET /api/v1/market/instruments` |
| tickers | `GET /api/v1/market/tickers` |
| order book | `GET /api/v1/market/books` |
| trades | `GET /api/v1/market/trades` |
| mark price | `GET /api/v1/market/mark-price` |
| funding rate | `GET /api/v1/market/funding-rate` |
| funding history | `GET /api/v1/market/funding-rate-history` |
| candles | `GET /api/v1/market/candles`, `/index-candles`, `/mark-price-candles` |
| position tiers | `GET /api/v1/market/position-tiers` |
| asset balances | `GET /api/v1/asset/balances` |
| transfer funds | `POST /api/v1/asset/transfer` |
| transfer/deposit/withdraw history | `GET /api/v1/asset/bills`, `/withdrawal-history`, `/deposit-history` |
| account config/currencies | `GET /api/v1/account/config`, `GET /api/v1/asset/currencies` |
| balances | `GET /api/v1/account/balance` |
| positions | `GET /api/v1/account/positions` |
| positions history | `GET /api/v1/account/positions-history` |
| margin mode | `GET /api/v1/account/margin-mode`, `POST /api/v1/account/set-margin-mode` |
| position mode | `GET /api/v1/account/position-mode`, `POST /api/v1/account/set-position-mode` |
| leverage | `GET /api/v1/account/leverage-info`, `GET /api/v1/account/batch-leverage-info`, `POST /api/v1/account/set-leverage` |
| place order | `POST /api/v1/trade/order` |
| batch place | `POST /api/v1/trade/batch-orders` |
| TPSL order | `POST /api/v1/trade/order-tpsl`, `POST /api/v1/trade/cancel-tpsl`, `GET /api/v1/trade/orders-tpsl-pending`, `GET /api/v1/trade/order-tpsl-detail`, `GET /api/v1/trade/orders-tpsl-history` |
| algo order | `POST /api/v1/trade/order-algo`, `POST /api/v1/trade/cancel-algo`, `GET /api/v1/trade/orders-algo-pending`, `GET /api/v1/trade/orders-algo-history` |
| cancel order | `POST /api/v1/trade/cancel-order` |
| batch cancel | `POST /api/v1/trade/cancel-batch-orders` |
| open orders | `GET /api/v1/trade/orders-pending` |
| query order | `GET /api/v1/trade/order-detail` |
| close position | `POST /api/v1/trade/close-position` |
| completed order history | `GET /api/v1/trade/orders-history` |
| recent fills | `GET /api/v1/trade/fills-history` |
| order price range | `GET /api/v1/trade/order/price-range` |
| public WS | `books`, `books5`, `trades`, `tickers`, `candle*`, `funding-rate` |
| private WS | `orders`, `orders-algo`, `positions`, `account` after `login` |

## Capability V2

- Product boundary: linear USDT perpetuals only. `market_types` is
  `[Perpetual]`; Spot is 项目未实现 Spot, and dated futures, inverse
  contracts, and options are not declared.
- Public REST: native symbol rules, order book snapshots, tickers/trades,
  funding current/history, candles, mark price, and position tiers.
- Private REST: native balances, positions, order lifecycle, query/open orders,
  fills history, native batch place/cancel, and composed cancel-all.
- Public/private WS runtime: subscription specs and parsers are present for
  public books/books5/funding and private orders/account/positions. Persistent
  socket supervision remains outside the adapter.
- Official WS order book detail: `books` sends an initial 200-depth snapshot
  and 100ms incremental updates; `books5` sends 5-depth snapshots every 100ms
  when changed. `books` includes `prevSeqId`/`seqId`, so `prevSeqId` must match
  the previous `seqId`; no checksum was found. Mapping still needs these
  interval/depth/sequence fields. Source batch:
  [WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).
- Order book strictness: REST and WS must be rebuilt from
  `GET /api/v1/market/books` on disconnect or sequence mismatch.
- Funding/open interest: funding current/history is supported. Open interest is
  explicitly unsupported because the verified OpenAPI used by this adapter has
  no stable endpoint.

## Runtime Policies

- Heartbeat: client sends text `ping` every 25 seconds, expects text `pong`
  within 10 seconds, and treats 60 seconds without messages as stale.
- Auth renewal: BloFin private WS uses signed login, not listen-key renewal.
  Reconnect requires login and resubscribe; failed relogin should reconnect.
- Rate limits: endpoint mapping declares conservative public REST, private REST,
  and order buckets. Runtime should classify HTTP 418/429 and BloFin code
  `50011` as rate-limited.
- Pagination: funding history and open orders use `before`/`after` cursor style
  with `limit <= 100`; fills history uses `begin`/`end` time range with
  `limit <= 100`; order book `size <= 100`.
- Reconciliation: unknown place/cancel outcomes should query
  `/api/v1/trade/order-detail`, then open orders, then fills history. Cancel-all
  composes open-order readback plus native batch cancel.
- Batch semantics: BloFin native batch place/cancel is declared as partial,
  non-atomic, max 20 items, same market type required, client order IDs allowed.

## Fixtures And Tests

Sanitized fixtures live in `tests/fixtures/exchanges/blofin/` and cover
instruments/contract boundary, order book, balance, position, order ack, fills,
funding, open-interest unsupported metadata, and private WS order payloads.
Adapter tests cover request-spec shape, signing vectors, public parser fixtures,
private body builders, and WS subscribe/parser helpers.

## Validation

Targeted offline validation:

```bash
CARGO_TARGET_DIR=/tmp/rustcta_blofin_target cargo test -p rustcta-exchange-gateway blofin --lib -- --nocapture
CARGO_TARGET_DIR=/tmp/rustcta_gateway_app_blofin_target cargo check -p rustcta-gateway
python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/blofin/endpoint_mapping.yaml
```
