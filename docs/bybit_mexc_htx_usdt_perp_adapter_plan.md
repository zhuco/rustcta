# Bybit / MEXC / HTX USDT Perpetual Adapter Plan

This document is the implementation contract for adding Bybit, MEXC, and HTX
USDT-margined perpetual support to the cross-exchange arbitrage runtime. The
target is Binance-aligned behavior at the strategy boundary: symbol discovery,
precision and minimum-order rules, public order book and funding data, private
REST trading, private WebSocket reconciliation, leverage, and position mode
preflight.

## Design Boundary

The project uses a unified exchange boundary:

- `ExchangeId` identifies venues in config, dashboards, routing, risk, market
  data, execution, and storage.
- `MarketDataAdapter` owns public REST discovery, funding, REST order book
  snapshots, public WebSocket subscription building, and public WebSocket
  message parsing.
- `TradingAdapter` owns private order placement, cancellation, order/fill
  query, positions, balances, leverage, position mode, fee readback, amend, and
  disconnect-cancel safety features.
- `PrivatePerpTradingAdapter` currently contains the exchange-specific private
  perpetual implementation for Bitget and Gate. Bybit, MEXC, and HTX should be
  added there or split into per-exchange private modules before enabling live
  orders.

Public market data is safe to enable without credentials after exchange
connectivity tests. Private trading must remain disabled until REST signing,
private WebSocket authentication, parser coverage, preflight, and small-live
credential tests pass.

## Symbol Formats

| Exchange | Strategy canonical | Venue symbol | Example |
| --- | --- | --- | --- |
| Binance | `BTC/USDT` | compact | `BTCUSDT` |
| Bybit | `BTC/USDT` | compact | `BTCUSDT` |
| MEXC | `BTC/USDT` | underscored | `BTC_USDT` |
| HTX | `BTC/USDT` | dashed contract code | `BTC-USDT` |

All three new exchanges must round-trip through
`exchange_symbol_for` and `canonical_from_exchange_symbol`, and instrument
metadata must carry `ContractType::LinearPerpetual`, `quote=USDT`, and
`settle_asset=USDT`.

## Public Market Data Scope

Implemented in this change:

- `ExchangeId::{Bybit,Mexc,Htx}` with serde/config parsing aliases.
- Symbol mapping for compact, underscored, and dashed venue symbols.
- Market adapters:
  - `src/exchanges/adapters/bybit_market.rs`
  - `src/exchanges/adapters/mexc_market.rs`
  - `src/exchanges/adapters/htx_market.rs`
- Server, observe, preflight, and live loader factory wiring.
- Public WebSocket endpoint construction for dashboard `public-ws` mode.
- Unit coverage for subscription building, order book parsing, and instrument
  rule parsing.

Bybit public API:

- Instruments: `GET /v5/market/instruments-info?category=linear`.
- Funding/mark/index: `GET /v5/market/tickers?category=linear`.
- REST order book: `GET /v5/market/orderbook?category=linear&symbol=BTCUSDT`.
- Public WebSocket: `wss://stream.bybit.com/v5/public/linear`,
  subscribe `orderbook.1.BTCUSDT`.
- Precision/minimums are parsed from `priceFilter.tickSize`,
  `lotSizeFilter.qtyStep`, `lotSizeFilter.minOrderQty`, and
  `lotSizeFilter.minNotionalValue`.

MEXC public API:

- Instruments: `GET /api/v1/contract/detail`.
- Funding: `GET /api/v1/contract/funding_rate/{symbol}`.
- REST order book: `GET /api/v1/contract/depth/{symbol}`.
- Public WebSocket: `wss://contract.mexc.com/edge`, subscribe
  `sub.depth.full` per symbol.
- Precision/minimums are parsed from `priceUnit`, `volUnit`, `minVol`,
  `contractSize`, and optional `minNotional`.

HTX public API:

- Instruments: `GET /linear-swap-api/v1/swap_contract_info`.
- Funding: `GET /linear-swap-api/v1/swap_funding_rate?contract_code=BTC-USDT`.
- REST order book:
  `GET /linear-swap-ex/market/depth?contract_code=BTC-USDT&type=step0`.
- Public WebSocket: `wss://api.hbdm.com/linear-swap-ws`, subscribe
  `market.BTC-USDT.depth.step0`.
- HTX WebSocket transport sends compressed frames in production. The adapter
  parses decompressed JSON payloads; the WebSocket runner may need gzip
  decompression before HTX can run continuously in `public-ws` mode.

## Private REST Target Matrix

The following methods are required before any of these venues can be enabled
for live orders:

| Capability | Bybit V5 | MEXC Futures | HTX Linear Swap |
| --- | --- | --- | --- |
| Place order | `POST /v5/order/create` | `POST /api/v1/private/order/submit` | `POST /linear-swap-api/v1/swap_cross_order` |
| Cancel order | `POST /v5/order/cancel` | `POST /api/v1/private/order/cancel` | `POST /linear-swap-api/v1/swap_cross_cancel` |
| Cancel all | `POST /v5/order/cancel-all` | private cancel-all endpoint | `POST /linear-swap-api/v1/swap_cross_cancelall` |
| Batch cancel | `POST /v5/order/cancel-batch` | private batch cancel endpoint | `POST /linear-swap-api/v1/swap_cross_cancel` batch-compatible path if supported |
| Amend order | `POST /v5/order/amend` | private order change endpoint if available | not mandatory unless supported and verified |
| Open orders | `GET /v5/order/realtime` | private open-orders endpoint | `POST /linear-swap-api/v1/swap_cross_openorders` |
| Recent fills | `GET /v5/execution/list` | private deals/order-deals endpoint | `POST /linear-swap-api/v1/swap_cross_matchresults` |
| Positions | `GET /v5/position/list` | private position endpoint | `POST /linear-swap-api/v1/swap_cross_position_info` |
| Balances | `GET /v5/account/wallet-balance` | private account asset endpoint | `POST /linear-swap-api/v1/swap_cross_account_info` |
| Leverage | `POST /v5/position/set-leverage` | private leverage endpoint | `POST /linear-swap-api/v1/swap_cross_switch_lever_rate` |
| Position mode | `POST /v5/position/switch-mode` | verify account-level support | HTX linear swap generally uses direction/offset fields; verify hedge/one-way semantics per account |
| Fee rate | `GET /v5/account/fee-rate` | private fee-rate endpoint if available | account fee endpoint if available |

Private REST implementation requirements:

- Request builders must be unit tested without credentials.
- Signing must be deterministic and covered by fixture tests.
- All order identifiers must preserve both client order id and exchange order
  id when the venue supports both.
- All quantity conversion must use `InstrumentMeta` contract size and lot
  rules, not hard-coded decimals.
- Position-side mapping must explicitly handle one-way and hedge mode. If a
  venue cannot switch mode while positions/orders exist, preflight must block
  live startup.
- REST order acknowledgements must be treated as accepted/rejected only; final
  state comes from private WebSocket and REST audit.

## Private WebSocket Target Matrix

Bybit:

- URL: `wss://stream.bybit.com/v5/private`.
- Auth: `op=auth`, args `[api_key, expires, signature]`.
- Topics: `order`, `execution`, `position`, `wallet`.

MEXC:

- URL: futures private WebSocket endpoint under contract API.
- Auth: verify current official login method and signature payload.
- Topics needed: orders, deals/fills, positions, assets.

HTX:

- URL: linear swap notification WebSocket endpoint.
- Auth: HMAC signed access key payload.
- Topics needed: orders, match orders/fills, positions, accounts.
- Transport compression/decompression must be verified in the runner.

Private WebSocket implementation requirements:

- Login builders and subscription builders must be tested as raw JSON.
- Parsers must emit existing `PrivateEvent` variants for order, fill, position,
  balance, heartbeat, disconnected, and venue error messages.
- Reconnect must resubscribe and trigger REST audit after reconnect.
- Heartbeat handling must use each exchange's native ping/pong protocol.
- Unknown private messages must be logged at low volume and never crash the
  stream task.

## Live-Small Readiness Gate

Before enabling Bybit/MEXC/HTX for real orders:

- Public REST instrument load succeeds from the production server.
- Public WebSocket receives fresh books for at least 50 symbols for 30 minutes.
- Private REST read-only preflight succeeds: balances, positions, open orders,
  recent fills, fee rate where available.
- Private WebSocket auth succeeds and emits heartbeat/account events for at
  least 30 minutes.
- Leverage and position mode preflight passes for the configured symbols.
- Dry-run order simulation uses real instrument precision/minimum rules.
- Live order kill switch, cancel-all, and REST audit are enabled.
- First live run uses `dry_run=false`, `RuntimeMode::LiveSmall`, order notional
  capped at 10 USDT, and portfolio cap of 50 total positions.

## Current Status

Market-data support for the three venues has been implemented and wired into
the public scanner path. Private trading is intentionally disabled for
Bybit/MEXC/HTX until the private REST/WS matrix above is implemented and tested
with operator credentials. This is a safety constraint, not a strategy-layer
limitation.
