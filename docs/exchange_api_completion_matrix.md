# Exchange API Completion Matrix

Workspace snapshot: 2026-06-06, based on current source inspection.

This matrix tracks whether exchange adapters match the repository's unified
contracts. It is intentionally code-first: old docs are not treated as
authoritative when they disagree with `src/`.

## Unified Baseline

Spot baseline is `ExchangeClient` in `src/exchanges/unified.rs`:

- symbol normalization and `load_symbol_rules`
- balances, order book, fee rate, recent fills
- place order, cancel order, query order, open orders
- public order-book stream and private user stream where the venue adapter
  claims support
- explicit `Unsupported` or classified errors for unavailable live behavior

USDT perpetual baseline is `TradingAdapter` plus `MarketDataAdapter`:

- public instruments/order book/funding/ticker routes
- place, cancel, cancel-all, batch cancel, query order, open orders
- fills, balances, positions, fee readback, symbol account config
- leverage, position mode, close position
- private order/fill/balance/position stream, or documented reconciliation
  fallback

Binance official reference points used for endpoint grouping:

- Spot REST docs: <https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md>
- USD-M futures endpoint reference: <https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info>

## Product-Line Scope

The Spot completion table only counts adapters that implement the unified
`ExchangeClient` Spot contract exported from `src/exchanges/mod.rs`: Binance,
OKX, Bitget, Gate.io, MEXC, CoinEx, KuCoin, and Paper. Bybit and HTX currently
exist in this workspace as market-data/private-perpetual paths, not as Spot
`ExchangeClient` adapters, so they are tracked under USDT perpetual coverage
instead of being treated as missing Spot rows.

The USDT perpetual completion table only counts exchanges registered through
the `private_perp` gateway path: Binance, OKX, Bitget, Gate, Bybit, MEXC, and
HTX. CoinEx and KuCoin currently have Spot clients only; registry tests keep
them out of `private_perp` until a side-effect-free USDT perpetual protocol is
implemented and verified.

## Spot Coverage

| Exchange | Path | Unified completion | Status |
| --- | --- | ---: | --- |
| Binance | `src/exchanges/binance/spot.rs` | 90-95% | Now covers symbol rules, balances, book, order submit/cancel/cancel-all/query/open orders, quote-order-qty market orders, keep-priority reduce-quantity amend, OCO/OTO order lists, fee API via current `/api/v3/account/commission` plus legacy trade-fee response parsing, recent fills, public WS, private user stream, dry-run acks, client order id validation across submit/quote/amend/cancel/order-list paths, and local HTTP mock coverage proving signed private REST routing for readbacks and order mutations. Remaining gaps are beyond the common 90% Spot baseline: live validation depth and optional niche order-list variants such as OTOCO/OPO. |
| OKX | `src/exchanges/okx/spot.rs` | 90-95% | Covers symbol rules, balances, book, order lifecycle including regular and quote-sized dry-run acks, `post_only`/IOC/FOK, base-currency market sizing, quote-sized market orders via `tgtCcy=quote_ccy`, unified quantity amend via `newSz`, cancel and cancel-all dry-run plus market-type validation, composed pending-order plus batch cancel-all, fee API, recent fills, public WS, private user stream order/fill/account/control parser coverage, and local HTTP mock coverage proving signed private REST readback and mutation routing for balances, query order, open orders with field-level assertions, fee rate, recent fills, order submit, quote-sized market submit, single cancel, composed cancel-all, and amend. Spot OCO/OTO order lists remain explicitly unsupported until mapped to verified OKX semantics, with OKX-specific tests proving shared order-list validation runs before unsupported. Remaining gap: broader live validation depth. |
| Bitget | `src/exchanges/bitget/mod.rs` | 90-95% | Covers rules, balances, book, orders including regular and quote-sized dry-run acks, `post_only`/IOC/FOK force mappings, quote-sized market buys, UTA quantity amend, symbol-scoped cancel-all, cancel and cancel-all dry-run plus market-type validation, fees including v3-to-v2 fallback, fills, public WS, and local HTTP mock coverage proving signed private REST readback and mutation routing for balances, query order, open orders with field-level assertions, fee rate, recent fills, order submit, quote-sized market submit, single cancel, cancel-all, and amend. Spot OCO/OTO order lists remain explicitly unsupported until mapped to verified Bitget semantics, with Bitget-specific tests proving shared order-list validation runs before unsupported. Private user stream runtime is explicitly unsupported with REST polling fallback, while parser-level fixtures now cover order/fill/balance/control payload normalization for future private stream enablement. |
| Gate.io | `src/exchanges/gateio/mod.rs` | 90-95% | Covers rules, balances, book, orders including regular and quote-sized dry-run acks, quote-sized market buys, order-id quantity amend, `poc` post-only/FOK and currency-pair cancel-all, cancel and cancel-all dry-run plus market-type validation, fees, fills, public WS, and local HTTP mock coverage proving signed private REST readback and mutation routing for balances, query order, open orders with field-level assertions, fee rate, recent fills, order submit, quote-sized market submit, single cancel, cancel-all, and amend. Spot OCO/OTO order lists remain explicitly unsupported until mapped to verified Gate.io semantics, with Gate.io-specific tests proving shared order-list validation runs before unsupported. Private user stream runtime is explicitly unsupported with REST polling fallback, while parser-level fixtures now cover order/fill/balance/control payload normalization for future private stream enablement. |
| MEXC | `src/exchanges/mexc/mod.rs` | 90-95% | Covers rules, balances, book, orders including regular and quote-sized dry-run acks, quote-sized market buys, explicit no-native-amend handling with Spot-only validation, symbol-scoped cancel-all, cancel and cancel-all dry-run plus market-type validation, fees, fills, public WS, and local HTTP mock coverage proving signed private REST readback and mutation routing for balances, query order, open orders with field-level assertions, fee rate, recent fills, order submit, quote-sized market submit, single cancel, cancel-all, and explicit amend unsupported handling. Spot OCO/OTO order lists remain explicitly unsupported until mapped to verified MEXC semantics, with MEXC-specific tests proving shared order-list validation runs before unsupported. Private user stream runtime is explicitly unsupported with REST polling fallback, while parser-level fixtures now cover order/fill/balance/control payload normalization for future private stream enablement. |
| CoinEx | `src/exchanges/coinex/mod.rs` | 90-95% | Covers rules, balances, book, orders including regular and quote-sized dry-run acks, quote-sized market buys, native quantity amend, v2 `maker_only`/IOC/FOK types with matching fallback rules, market-scoped cancel-all, cancel and cancel-all dry-run plus market-type validation, fees, fills, public WS, and local HTTP mock coverage proving signed private REST readback and mutation routing for balances, query order, open orders with field-level assertions, fee rate, recent fills, order submit, quote-sized market submit, single cancel, cancel-all, and amend. Spot OCO/OTO order lists remain explicitly unsupported until mapped to verified CoinEx semantics, with CoinEx-specific tests proving shared order-list validation runs before unsupported. Private user stream runtime is explicitly unsupported with REST polling fallback, while parser-level fixtures now cover order/fill/balance/control payload normalization for future private stream enablement. |
| KuCoin | `src/exchanges/kucoin/mod.rs` | 90-95% | Covers rules, balances, book, order submit/cancel/cancel-all/query/open orders including regular and quote-sized dry-run acks, quote-sized market buys, quantity amend via HF `orders/alter`, cancel and cancel-all dry-run plus market-type validation, post-only/IOC/FOK HF order semantics, fees, fills, public WS, private user stream via private bullet token, and local HTTP mock coverage proving signed private REST readback and mutation routing for balances, query order, open orders with field-level assertions, fee rate, recent fills, HF order submit, quote-sized market submit, single cancel, symbol cancel-all, and amend. Spot OCO/OTO order lists remain explicitly unsupported until mapped to verified KuCoin semantics, with KuCoin-specific tests proving shared order-list validation runs before unsupported. Remaining gap: private stream needs live validation depth comparable to Binance/OKX before relying on it without REST reconciliation fallback. |
| Paper | `src/exchanges/paper/mod.rs`, `crates/rustcta-exchange-gateway/src/adapters/paper.rs` | N/A | Internal deterministic execution harness, not an external exchange benchmark. The legacy paper path remains the strategy simulation baseline, and the industrial workspace gateway now has an in-memory paper adapter implementing the typed local gateway surface for local end-to-end runs: balances, books, fees, fills, place/cancel, batch place, batch cancel, cancel-all, query/open-order readback, and public/private subscription acknowledgements. Advanced Spot quote/amend/order-list flags remain false unless a concrete venue adapter implements them. Unified boundary checks now cover blank order ids, cancel client order ids, fee-rate symbols, empty orderbook subscription lists, and zero-depth orderbook requests so simulations do not bypass the shared `ExchangeClient` request contract. |

## Legacy / Out-Of-Baseline Adapters

These adapters are exported from `src/exchanges/mod.rs` but are not counted in
the Spot `ExchangeClient` or USDT perpetual `private_perp` completion
percentages until they are migrated to those unified contracts.

| Exchange | Path | Current contract | Status |
| --- | --- | --- | --- |
| BitMart | `src/exchanges/bitmart/mod.rs` | Legacy `Exchange` | Broad mixed Spot/Futures implementation with balances, books, order submit/cancel/query/open/history, trades, fees, positions, leverage, cancel-all, batch/plan-order handling, symbol discovery, and websocket client construction. It is not wired to `ExchangeClient`, `TradingAdapter`, or the side-effect-free `private_perp` protocol, so Binance-parity completion is not comparable yet. Migration should start by splitting Spot and Futures surfaces and adding offline request-spec tests before claiming unified coverage. |
| Hyperliquid | `src/exchanges/hyperliquid/mod.rs` | Legacy `Exchange` | Hyperliquid USDC perpetual adapter with custom signing, account/balance reads, ticker/book, order submit/cancel/query/open/history, trades, klines/statistics, fee readback, positions, leverage, composed cancel-all, batch orders, symbol discovery, and websocket client construction. It is outside the Binance USD-M CEX baseline and is not registered in the `private_perp` gateway path; track it as a venue-specific migration target instead of a missing Binance-parity row. |

## Binance Spot Changes In This Pass

Implemented and tested in `src/exchanges/binance/spot.rs`:

- `load_symbol_rules()` via `GET /api/v3/exchangeInfo`
- `get_recent_fills()` via `GET /api/v3/myTrades`
- `get_fee_rate()` via current `GET /api/v3/account/commission`, while
  retaining parser compatibility for legacy `/sapi/v1/asset/tradeFee` arrays
  and account-level `commissionRates` responses.
- `cancel_all_orders()` via signed `DELETE /api/v3/openOrders` for the newly
  added unified cancel-all method; the parser counts both ordinary orders and
  `orderReports` nested under OCO list responses.
- `place_quote_market_order()` via signed `POST /api/v3/order` using
  `quoteOrderQty` for Binance Spot market orders, while other `ExchangeClient`
  adapters keep the default explicit unsupported response until venue-specific
  quote sizing is wired.
- `amend_order()` via signed `PUT /api/v3/order/amend/keepPriority` for
  Binance Spot keep-priority quantity reductions, with a narrow
  `AmendOrderRequest` model so other adapters can keep returning explicit
  unsupported until they have verified native amend semantics.
- `place_order_list()` via signed `POST /api/v3/orderList/oco` and
  `POST /api/v3/orderList/oto`, with a narrow `OrderListRequest` model for
  Binance Spot OCO/OTO order lists. Other adapters keep returning explicit
  unsupported until their native order-list semantics are mapped; shared
  default paths now validate `QuoteMarketOrderRequest`, `AmendOrderRequest`,
  `OrderListRequest`, `CancelAllOrdersRequest`, and public orderbook
  subscription/recent-fill symbols before returning unsupported so invalid
  market-type/symbol/quantity/order-id/client-order-id and order-list leg
  price/stop-price errors are not hidden by capability fallback.
- Binance Spot, OKX, Bitget, Gate.io, MEXC, CoinEx, KuCoin, Paper, and the
  legacy unified `ExchangeClient` wrapper now reject zero-depth orderbook
  requests before REST/legacy delegation, so invalid public book requests are
  not silently normalized into venue defaults.
- `ExchangeClientCapabilities` now advertises advanced Spot capabilities for
  quote-sized market orders, native amend, and order-list placement separately,
  so runtime routing can distinguish implemented venue mappings from explicit
  unsupported fallbacks.
- Binance Spot capabilities now advertise private user-stream support because
  `subscribe_user_stream()` is implemented.
- Binance Spot private readbacks now have local HTTP mock routing coverage:
  `get_balances()`, `get_order()`, `get_open_orders()`, `get_fee_rate()`, and
  `get_recent_fills()` are verified to send signed REST requests with
  `X-MBX-APIKEY`, `timestamp`, `recvWindow`, `signature`, and endpoint-specific
  query fields, then normalize responses into the shared `ExchangeClient`
  models.
- Binance Spot private order mutations now have local HTTP mock routing
  coverage: `place_order()`, `place_quote_market_order()`, `cancel_order()`,
  `cancel_all_orders()`, `amend_order()`, and `place_order_list()` are verified
  to send signed REST requests to the documented order endpoints with
  endpoint-specific query fields, then normalize responses into the shared
  order and order-list models.

Validation run:

```bash
cargo test binance_ --lib
cargo test binance_spot_client_should_route_common_private_rest_readbacks --lib
cargo test binance_spot_client_should_route_order_mutations --lib
```

Result: `cargo test binance_ --lib` reports 54 passed tests in the current
workspace. This is a full library name filter, so it includes Binance Spot,
Binance market adapters, Binance private-perp tests, and Binance-named strategy
tests. The latest targeted Spot readback and order-mutation routing tests also
passed.

## OKX Spot Changes In This Pass

Implemented and tested in `src/exchanges/okx/spot.rs`:

- `load_symbol_rules()` via public `GET /api/v5/public/instruments?instType=SPOT`
- `get_recent_fills()` via signed `GET /api/v5/trade/fills-history?instType=SPOT`
- `cancel_all_orders()` by querying `GET /api/v5/trade/orders-pending` and
  submitting `POST /api/v5/trade/cancel-batch-orders` in chunks of 20 orders,
  matching the official OKX batch-cancel limit.
  Dry-run returns an offline cancel-all ack and rejects non-Spot market types
  before pending-order readback.
- `amend_order()` via signed `POST /api/v5/trade/amend-order`, mapping the
  shared `AmendOrderRequest.new_quantity` to OKX `newSz`. Dry-run coverage now
  locks the client-order-id fallback path and rejects invalid replacement
  client ids before request-body construction.
- `place_order()` dry-run now validates the local OKX request body mapping
  before returning an offline submit ack.
- `place_quote_market_order()` via signed `POST /api/v5/trade/order`, mapping
  the shared quote amount to OKX `sz` with `tgtCcy=quote_ccy`.
- `cancel_order()` returns an offline cancellation ack in dry-run mode and
  rejects non-Spot market types and invalid client order ids before
  constructing OKX cancel-order bodies.
- OKX Spot capabilities now advertise private user-stream support because
  `subscribe_user_stream()` is implemented.
- OKX Spot private user-stream parser coverage now includes account balance
  events plus login/subscribe control message filtering, in addition to the
  existing order/fill fixture.
- `TimeInForce::GTX` now maps to OKX `ordType=post_only`, matching the
  shared Spot convention used by Binance-like adapters; market orders remain
  base-currency sized via `tgtCcy=base_ccy`.
- OKX Spot private readbacks now have local HTTP mock routing coverage:
  `get_balances()`, `get_order()`, `get_open_orders()`, `get_fee_rate()`, and
  `get_recent_fills()` are verified to send signed REST requests with
  `OK-ACCESS-KEY`, `OK-ACCESS-SIGN`, `OK-ACCESS-TIMESTAMP`,
  `OK-ACCESS-PASSPHRASE`, and endpoint-specific query fields, then normalize
  responses into the shared `ExchangeClient` models. Open-order readback
  coverage also asserts client order id, side/status, price, quantity, filled
  quantity, and average-price absence so list parsing stays aligned with
  single-order parsing.
- OKX Spot private mutations now have local HTTP mock routing coverage:
  `place_order()`, `place_quote_market_order()`, `cancel_order()`,
  `cancel_all_orders()`, and `amend_order()` are verified against signed
  `POST /api/v5/trade/order`, `POST /api/v5/trade/cancel-order`,
  `GET /api/v5/trade/orders-pending`,
  `POST /api/v5/trade/cancel-batch-orders`, and
  `POST /api/v5/trade/amend-order` requests. OKX still reports
  `supports_order_list=false` because Spot OCO/OTO order lists are not mapped
  to a verified unified route; OKX-specific order-list tests now prove shared
  request validation and client-order-id validation run before the explicit
  unsupported fallback.

Validation run:

```bash
cargo test okx_ --lib
cargo test okx_spot_client_should_route_common_private_rest_readbacks --lib
cargo test okx_spot_client_should_route_order_mutations --lib
```

Result: 56 OKX-filtered lib tests passed in the current workspace after the
latest targeted Spot readback and mutation routing tests passed.

## Gate.io Spot Changes In This Pass

Implemented and tested in `src/exchanges/gateio/mod.rs`:

- `cancel_all_orders()` via signed `DELETE /spot/orders?currency_pair=...`.
- Gate.io Spot capabilities now advertise `supports_cancel_all_orders`.
- Cancel-all response counting treats rows with an empty/missing `message` and
  `label` as successful cancellations, matching Gate's per-order result array.
  Dry-run returns an offline cancel-all ack and rejects non-Spot market types
  before constructing the `currency_pair` query.
- `place_quote_market_order()` maps shared quote-sized market buys to signed
  `POST /spot/orders` with `type=market`, `side=buy`, and `amount` as quote
  currency quantity, matching Gate.io's documented market-buy semantics.
- `place_order()` dry-run now validates the local Gate.io request body mapping
  before returning an offline submit ack, without reading symbol rules from the
  exchange.
- `amend_order()` maps shared order-id quantity amendments to signed
  `PATCH /spot/orders/{order_id}` with `amount`, matching Gate.io's native
  amount-amend semantics. Client-order-id-only amend and `new_client_order_id`
  remain explicitly unsupported after local client id validation because the
  REST amend path requires exchange `order_id` and Gate's `amend_text` is not a
  client-order-id replacement.
- `cancel_order()` now validates `MarketType::Spot` and client order id format
  before constructing the Gate.io Spot cancel path and `currency_pair` query.
  Client-order-id-only cancel remains explicitly unsupported because the REST
  cancel endpoint requires exchange `order_id`.
- Gate.io Spot private readbacks now have local HTTP mock routing coverage:
  `get_balances()`, `get_order()`, `get_open_orders()`, `get_fee_rate()`, and
  `get_recent_fills()` are verified to send signed REST requests with `KEY`,
  `Timestamp`, `SIGN`, `Content-Type`, and endpoint-specific query fields
  under the production-like `/api/v4` base path, then normalize responses into
  the shared `ExchangeClient` models. Open-order readback coverage also asserts
  side, price, quantity, filled quantity, average price, and client order id so
  the nested `/spot/open_orders` envelope stays aligned with single-order
  parsing.
- Gate.io Spot private mutations now have local HTTP mock routing coverage:
  `place_order()`, `place_quote_market_order()`, `cancel_order()`,
  `cancel_all_orders()`, and `amend_order()` are verified to send signed REST
  requests against `POST /api/v4/spot/orders`,
  `DELETE /api/v4/spot/orders/{order_id}`, `DELETE /api/v4/spot/orders`, and
  `PATCH /api/v4/spot/orders/{order_id}`, including currency-pair, balance, and
  fee preflight routing. Order-list support remains explicitly false; Gate.io
  specific order-list tests now prove shared market-type, symbol, leg
  price/stop-price, and client-order-id validation run before the explicit
  unsupported fallback.
- Gate.io Spot private user-stream runtime remains explicitly unsupported with
  REST polling fallback, but parser-level fixtures now normalize
  representative private order, fill, balance, and control/error payloads into
  shared `UserStreamEvent` values.

Validation run:

```bash
cargo test gateio --lib
cargo test gateio_spot_client_should_route_common_private_rest_readbacks --lib
cargo test gateio_spot_client_should_route_order_mutations --lib
```

Result: 21 Gate.io-filtered tests passed in the current workspace. The latest
targeted Spot readback and mutation routing tests also passed.

## CoinEx Spot Changes In This Pass

Implemented and tested in `src/exchanges/coinex/mod.rs`:

- `cancel_all_orders()` via signed `POST /spot/cancel-all-order` with
  `market_type=SPOT` and required market scope.
- CoinEx Spot capabilities now advertise `supports_cancel_all_orders`.
- The current V2 endpoint returns empty `data`, so `cancelled_orders` remains
  `0` unless a future response shape includes explicit order rows.
  Dry-run returns an offline cancel-all ack and rejects non-Spot market types
  before constructing the v2 cancel-all body.
- `place_quote_market_order()` maps shared quote-sized market buys to signed
  `POST /spot/order` with `type=market`, `amount` as quote quantity, and
  `ccy=<quote_asset>`, matching CoinEx v2's documented market-order currency
  selection.
- `place_order()` dry-run now validates the local CoinEx v2 request body
  mapping before returning an offline submit ack, without reading symbol rules
  from the exchange.
- `amend_order()` maps shared order-id quantity amendments to signed
  `POST /spot/modify-order` with `market_type=SPOT` and `amount`. Client-id-only
  amend and assigning a new client id remain explicitly unsupported after local
  client id validation because the CoinEx endpoint requires numeric exchange
  `order_id` and only accepts `amount`/`price` modifications.
- `cancel_order()` now rejects non-Spot market types and invalid client order
  ids before request body construction, matching the shared Spot-only request
  contract used by the other Spot adapters.
- Fallback symbol rules now advertise the same `PostOnly`/IOC/FOK and GTX
  order-type parity as parsed CoinEx v2 market metadata.
- CoinEx Spot private readbacks now have local HTTP mock routing coverage:
  `get_balances()`, `get_order()`, `get_open_orders()`, `get_fee_rate()`, and
  `get_recent_fills()` are verified to send signed REST requests with
  `X-COINEX-KEY`, `X-COINEX-SIGN`, `X-COINEX-TIMESTAMP`, `Content-Type`, and
  endpoint-specific query fields under the production-like `/v2` base path,
  then normalize V2 response envelopes into the shared `ExchangeClient` models.
  Open-order readback coverage also asserts client order id, side/status,
  price, quantity, filled quantity, and average-price absence so list parsing
  stays aligned with single-order parsing.
- CoinEx Spot private mutations now have local HTTP mock routing coverage:
  `place_order()`, `place_quote_market_order()`, `cancel_order()`,
  `cancel_all_orders()`, and `amend_order()` are verified to send signed REST
  requests against `POST /v2/spot/order`, `DELETE /v2/spot/order`,
  `POST /v2/spot/cancel-all-order`, and `POST /v2/spot/modify-order`,
  including symbol-rule, balance, and fee preflight routing. Order-list support
  remains explicitly false; CoinEx-specific order-list tests now prove shared
  market-type, symbol, leg price/stop-price, and client-order-id validation run
  before the explicit unsupported fallback.
- CoinEx Spot private user-stream runtime remains explicitly unsupported with
  REST polling fallback, but parser-level fixtures now normalize
  representative private order, fill, balance, and control/ack payloads into
  shared `UserStreamEvent` values.

Validation run:

```bash
cargo test coinex --lib
cargo test coinex_spot_client_should_route_common_private_rest_readbacks --lib
cargo test coinex_spot_client_should_route_order_mutations --lib
```

Result: 27 passed, 2 live-gated ignored in the current workspace. The latest
targeted Spot readback and mutation routing tests also passed.

## Bitget Spot Changes In This Pass

Implemented and tested in `src/exchanges/bitget/mod.rs`:

- `cancel_all_orders()` via signed
  `POST /api/v2/spot/trade/cancel-symbol-order` with required symbol scope.
- Bitget Spot capabilities now advertise `supports_cancel_all_orders`.
- Bitget's endpoint submits cancellation asynchronously and returns the symbol,
  so `cancelled_orders` remains `0`; actual cancellation should be confirmed
  through order history/open-order readback. Mutation mock coverage now asserts
  the shared ack keeps `cancelled_orders=0` and the signed cancel-symbol body
  contains only the required symbol.
  Dry-run returns an offline cancel-all ack and rejects non-Spot market types
  before constructing the cancel-symbol payload.
- `cancel_order()` rejects non-Spot market types and invalid client order ids
  before returning a dry-run ack or constructing the v2 cancel-order body.
- `place_quote_market_order()` maps shared quote-sized market buys to v2
  `size` and UTA `qty`, matching Bitget's market-buy quote coin semantics.
- `place_order()` dry-run now validates the local v2 request-body mapping before
  returning an offline submit ack, without reading symbol rules from the
  exchange.
- `amend_order()` maps shared quantity amendments to UTA v3 signed
  `POST /api/v3/trade/modify-order` with `category=SPOT` and `qty`. Classic
  v2 `cancel-replace-order` is intentionally not used for the shared model
  because it requires `price` as well as `size`. `new_client_order_id` is
  validated before returning the explicit unsupported response.
- `get_fee_rate()` now has offline parser coverage for both v3
  `/api/v3/account/fee-rate` response envelopes and the v2
  `/api/v2/common/trade-rate` fallback shape, preserving Binance-like fee API
  readback semantics under the shared Spot contract.
- Bitget Spot private readbacks now have local HTTP mock routing coverage:
  `get_balances()`, `get_order()`, `get_open_orders()`, `get_fee_rate()`, and
  `get_recent_fills()` are verified to send signed REST requests with
  `ACCESS-KEY`, `ACCESS-SIGN`, `ACCESS-TIMESTAMP`, `ACCESS-PASSPHRASE`,
  `locale=en-US`, `Content-Type`, and endpoint-specific query fields, then
  normalize Bitget response envelopes into the shared `ExchangeClient` models.
  Open-order readback coverage also asserts client order id, side/status,
  price, quantity, filled quantity, and average-price absence so list parsing
  stays aligned with single-order parsing.
- Bitget Spot fee readback now has local HTTP mock fallback routing coverage:
  a v3 `/api/v3/account/fee-rate` `40404` response is verified to retry signed
  v2 `/api/v2/common/trade-rate` with `businessType=spot` and parse the v2 fee
  envelope.
- Bitget Spot private mutations now have local HTTP mock routing coverage:
  `place_order()`, `place_quote_market_order()`, `cancel_order()`,
  `cancel_all_orders()`, and `amend_order()` are verified against signed
  `POST /api/v2/spot/trade/place-order`,
  `POST /api/v2/spot/trade/cancel-order`,
  `POST /api/v2/spot/trade/cancel-symbol-order`, and
  `POST /api/v3/trade/modify-order` requests. The test also covers the
  required preflight symbol-rule, balance, and fee-rate readbacks used before
  live order placement, and keeps `supports_order_list=false` explicit because
  Bitget Spot OCO/OTO order lists are not mapped to a verified unified route;
  Bitget-specific order-list tests now prove shared market-type, symbol, leg
  price/stop-price, and client-order-id validation run before the explicit
  unsupported fallback.
- `cancel_order()` now validates `MarketType::Spot` and client order id format
  before constructing the Bitget Spot cancel payload, matching the shared
  Spot-only request contract used by quote-market, amend, and cancel-all paths.
- `get_order()` now rejects blank order ids before constructing the Bitget Spot
  order lookup request, matching the shared Spot readback contract.
- Bitget Spot private user-stream runtime remains explicitly unsupported with
  REST polling fallback, but parser-level fixtures now normalize representative
  private order, fill, balance, and control/pong payloads into shared
  `UserStreamEvent` values.

Validation run:

```bash
cargo test bitget --lib
cargo test bitget_spot_client_should --lib
cargo test bitget_spot_client_should_route_order_mutations --lib
```

Result: 53 Bitget-filtered tests passed in the current workspace. The latest
targeted Spot readback, fee-fallback, and mutation routing tests also passed.

## Spot Readback Boundary Pass

Implemented and tested across tracked `ExchangeClient` single-order readback
entrypoints:

- Shared `validate_order_lookup_id()` rejects blank single-order lookup ids.
- Binance, OKX, Bitget, Gate.io, MEXC, CoinEx, and KuCoin Spot `get_order()`
  now validate `order_id` before constructing signed REST query params or URL
  paths, so invalid readbacks fail locally with `Validation` instead of
  leaking into venue-specific request construction.
- `LegacyExchangeClient` and `PaperExchangeClient` now use the same helper so
  compatibility and in-memory clients preserve the unified readback boundary.

Validation run:

```bash
cargo test order_lookup_id_should_reject_blank_values --lib
cargo test get_order_should_validate_order_id_before_request --lib
cargo test bitget_get_order_should_validate_order_id_before_request --lib
cargo test legacy_exchange_client_should_validate_get_order_id_before_lookup --lib
cargo test paper_get_order_should_validate_order_id_before_lookup --lib
cargo test bitget_ --lib
```

Result: shared lookup-id validation passed, Bitget/legacy/paper entrypoint
coverage passed, and 46 Bitget-filtered tests passed in the current workspace.

## Spot Request Boundary Pass

Implemented and tested in `src/exchanges/paper/mod.rs`:

- `get_fee_rate()` now validates the symbol before returning configured maker
  and taker fees.
- `subscribe_orderbook()` now rejects empty symbol lists before registering an
  in-memory subscriber, matching the default public-stream boundary.
- `cancel_order()` now validates optional client order ids before in-memory
  lookup, and `LegacyExchangeClient` applies the same cancel client-id boundary
  before delegating to legacy exchange implementations.
- `CancelOrderRequest` now exposes trimmed identifier accessors so blank
  `order_id` values do not mask valid `client_order_id` fallbacks. Paper
  cancellation and Binance, OKX, Bitget, MEXC, CoinEx, and KuCoin Spot cancel
  request construction use the filtered identifiers; dry-run cancel acks also
  return filtered ids so blank strings are not reported as active identifiers.
- `AmendOrderRequest` now exposes the same trimmed identifier accessors.
  Binance, OKX, Bitget, and KuCoin amend request construction ignores blank
  `order_id`/`new_client_order_id` fields while preserving valid client-order-id
  selectors; Gate.io and CoinEx no-client-id amend fallbacks reject blank
  exchange order ids before endpoint-specific body construction.
- Spot amend/cancel client-id validation helpers now use the filtered request
  accessors, so blank optional `client_order_id` or `new_client_order_id` fields
  are treated as absent consistently across validation, dry-run acks, and REST
  request construction.
- The default `ExchangeClient::amend_order()` unsupported fallback also uses
  the filtered amend accessors, so clients without native amend support share
  the same optional-id boundary before returning explicit unsupported.

Validation run:

```bash
cargo test paper_fee_rate_should_validate_symbol --lib
cargo test paper_orderbook_subscription_should_reject_empty_symbols --lib
cargo test paper_cancel_order_should_validate_client_order_id_before_lookup --lib
cargo test paper_cancel_order_should_ignore_blank_order_id_when_client_id_is_present --lib
cargo test cancel_order_request_should_ignore_blank_identifiers_for_lookup --lib
cargo test binance_cancel_order_should_validate_client_order_id_in_dry_run --lib
cargo test okx_cancel_order_should_ack_and_validate_market_type_in_dry_run --lib
cargo test bitget_cancel_order_should_validate_market_type_in_dry_run --lib
cargo test mexc_cancel_order_should_validate_market_type_in_dry_run --lib
cargo test coinex_cancel_order_should_validate_market_type_in_dry_run --lib
cargo test kucoin_cancel_order_should_ack_and_validate_market_type_in_dry_run --lib
cargo test legacy_exchange_client_should_validate_cancel_client_order_id_before_lookup --lib
cargo test legacy_exchange_client_should_validate_orderbook_depth_before_lookup --lib
cargo test paper_orderbook_should_validate_depth_before_lookup --lib
cargo test amend_order_request_should_ignore_blank_identifiers --lib
cargo test default_advanced_order_paths_should_validate_before_unsupported --lib
cargo test binance_orderbook_should_validate_depth_before_request --lib
cargo test okx_orderbook_should_validate_depth_before_request --lib
cargo test bitget_orderbook_should_validate_depth_before_request --lib
cargo test gateio_orderbook_should_validate_depth_before_request --lib
cargo test mexc_orderbook_should_validate_depth_before_request --lib
cargo test coinex_orderbook_should_validate_depth_before_request --lib
cargo test kucoin_orderbook_should_validate_depth_before_request --lib
cargo test binance_amend_order_params_should_use_keep_priority_fields --lib
cargo test okx_amend_order_body_should_use_new_size --lib
cargo test bitget_uta_amend_order_body_should_use_qty --lib
cargo test gateio_amend_order_body_should_use_amount --lib
cargo test coinex_amend_order_body_should_use_modify_order_contract --lib
cargo test kucoin_amend_order_body_should_use_new_size --lib
cargo test paper_ --lib
cargo test binance_ --lib
cargo test okx_ --lib
cargo test bitget_ --lib
cargo test gateio --lib
cargo test mexc_ --lib
cargo test coinex --lib
cargo test kucoin --lib
```

Result: Paper/legacy/Binance/non-Binance Spot boundary tests passed. Filtered
suites passed in the current workspace: Paper 27, Binance 41, OKX 56, Bitget
53, Gate.io 21, MEXC 47 passed plus 2 live-gated ignored, CoinEx 28 passed
plus 2 live-gated ignored, and KuCoin 23.

## Spot Order-Type Parity Pass

The Binance Spot parity baseline treats limit variants as common order
semantics even when individual venues encode them differently. The current
source now has offline coverage for these mappings:

| Exchange | Venue encoding | Validation |
| --- | --- | --- |
| OKX | `ordType=post_only/ioc/fok`; `GTX` maps to `post_only`; regular order dry-run validates local body mapping before returning an offline submit ack; ordinary market `sz` uses base currency through `tgtCcy=base_ccy`; quote-sized market orders use `tgtCcy=quote_ccy`; amend dry-run covers order-id and client-id selector paths plus invalid replacement client-id validation; cancel and cancel-all dry-run return offline cancellation acks and reject non-Spot market types before body/readback construction; cancel-all is composed from pending-order readback plus batch-cancel chunks; signed private REST readback routing includes open-order client id, side/status, price, quantity, filled quantity, and average-price absence assertions; zero-depth orderbook and blank single-order readback requests are rejected before REST. | `cargo test okx_ --lib` -> 56 passed |
| Bitget | v2 `force=post_only/ioc/fok`; regular order dry-run validates local v2 body mapping before returning an offline submit ack; UTA fallback `timeInForce=post_only/ioc/fok`; fallback rules advertise the same capability; market-buy quote amount maps to v2 `size` and UTA `qty`; quantity amend maps to UTA `modify-order` `qty`; fee-rate parsing covers v3 and v2 fallback response shapes, with signed v3-to-v2 fallback routing covered by local HTTP mock tests; invalid amend/cancel client order ids are rejected before unsupported/body construction; `POST /api/v2/spot/trade/cancel-symbol-order` submits symbol cancel-all asynchronously and keeps shared `cancelled_orders=0`; signed private REST readback routing includes open-order client id, side/status, price, quantity, filled quantity, and average-price absence assertions; Spot cancel and cancel-all dry-run reject non-Spot market types before payload construction; zero-depth orderbook requests are rejected before REST. | `cargo test bitget --lib` -> 53 passed |
| Gate.io | Spot `time_in_force=poc/ioc/fok`; regular order dry-run validates local body mapping before returning an offline submit ack; parsed symbol rules and fallback rules advertise post-only/FOK/GTX; market-buy quote amount maps to `POST /spot/orders` `amount`; quantity amend maps to `PATCH /spot/orders/{order_id}` `amount`; invalid amend/cancel client order ids are rejected before unsupported/path construction; `DELETE /spot/orders` cancel-all counts successful result entries; Spot cancel and cancel-all dry-run reject non-Spot market types before path/query construction, and cancel rejects client-id-only requests before using `/spot/orders/{order_id}`; signed private REST readback and mutation routing is covered by local HTTP mock tests; zero-depth orderbook and blank single-order readback requests are rejected before REST. | `cargo test gateio --lib` -> 21 passed |
| CoinEx | v2 `type=maker_only/ioc/fok`; no legacy `option` field for v2 order creation; regular order dry-run validates local v2 body mapping before returning an offline submit ack; fallback rules advertise the same post-only/IOC/FOK/GTX capability; market-buy quote amount maps to `POST /spot/order` `amount` plus `ccy=<quote_asset>`; quantity amend maps to `POST /spot/modify-order` `amount`; invalid amend/cancel client order ids are rejected before unsupported/body construction; `POST /spot/cancel-all-order` uses `market_type=SPOT`; Spot cancel and cancel-all dry-run reject non-Spot market types before request body construction; signed private REST readback routing includes open-order client id, side/status, price, quantity, filled quantity, and average-price absence assertions; zero-depth orderbook and blank single-order readback requests are rejected before REST. | `cargo test coinex --lib` -> 28 passed, 2 live-gated ignored |
| MEXC | Binance-like `LIMIT_MAKER/IOC/FOK` order types; regular order dry-run validates local signed order params before returning an offline submit ack; market-buy quote amount maps to `POST /api/v3/order` `quoteOrderQty`; amend is explicitly unsupported for Spot because Spot V3 has no documented native amend/cancel-replace endpoint, while non-Spot amend requests and invalid amend client order ids are rejected before unsupported; signed `DELETE /api/v3/openOrders` cancel-all has order-list response counting; Spot cancel and cancel-all dry-run reject non-Spot market types before signed query construction, and cancel rejects invalid client order ids before using `origClientOrderId`; signed private REST readback routing includes open-order client id, side/status, price, quantity, filled quantity, and average-price absence assertions; zero-depth orderbook and blank single-order readback requests are rejected before REST. | `cargo test mexc --lib` -> 47 passed, 2 live-gated ignored |
| KuCoin | HF orders use `postOnly=true` plus `timeInForce=IOC/FOK`; `GTX` maps to `postOnly=true`; regular order dry-run returns an offline submit ack after local HF body validation; market-buy quote amount maps to HF `funds`; quantity amend maps to HF `orders/alter` `newSize`; invalid amend/cancel client order ids are rejected before unsupported/URL construction; HF cancel and cancel-all dry-run return offline cancellation acks and reject non-Spot market types before signed REST deletion, and cancel rejects invalid client order ids before using `/client-order/{clientOid}`; signed private REST readback routing includes open-order client id, side/status, price, quantity, filled quantity, and average-price absence assertions; zero-depth orderbook and blank single-order readback requests are rejected before REST. | `cargo test kucoin --lib` -> 23 passed |

## MEXC Spot Changes In This Pass

Implemented and tested in `src/exchanges/mexc/mod.rs`:

- `cancel_all_orders()` via signed `DELETE /api/v3/openOrders` with required
  symbol scope, matching the current MEXC Spot V3 contract.
- MEXC Spot capabilities now advertise `supports_cancel_all_orders`.
- Cancel-all response counting covers ordinary order items and nested
  `orderReports` items so OCO-style response shapes do not undercount.
- `place_quote_market_order()` maps shared quote-sized market buys to signed
  `POST /api/v3/order` with `type=MARKET` and `quoteOrderQty`, matching MEXC's
  documented Binance-like market-buy quote-order semantics.
- `place_order()` dry-run now validates the local signed order parameter
  mapping before returning an offline submit ack, without reading symbol rules
  from the exchange.
- `amend_order()` validates the shared amend request and returns explicit
  unsupported because MEXC Spot V3 has no documented native amend or
  cancel-replace endpoint; callers should cancel and place a new order instead.
  Non-Spot amend requests are rejected by the shared market-type validation
  before reaching the unsupported Spot response, and invalid amend client order
  ids are rejected before unsupported is returned. Regression coverage also
  proves non-Spot amend requests with replacement client ids still fail on
  `market_type` before client-id replacement or unsupported handling.
- `cancel_order()` now validates `MarketType::Spot` and client order id format
  before constructing the signed MEXC Spot cancel query, matching the shared
  Spot-only request contract used by quote-market and cancel-all paths.
- `cancel_all_orders()` dry-run now returns an offline cancel-all ack and
  rejects non-Spot market types before signed query construction.
- MEXC Spot private readbacks now have local HTTP mock routing coverage:
  `get_balances()`, `get_order()`, `get_open_orders()`, `get_fee_rate()`, and
  `get_recent_fills()` are verified to send signed REST requests with
  `X-MEXC-APIKEY`, `timestamp`, `recvWindow`, `signature`, and endpoint-specific
  query fields, then normalize responses into the shared `ExchangeClient`
  models. Open-order readback coverage also asserts client order id,
  side/status, price, quantity, filled quantity, and average-price absence so
  list parsing stays aligned with single-order parsing.
- MEXC Spot private mutations now have local HTTP mock routing coverage:
  `place_order()`, `place_quote_market_order()`, `cancel_order()`, and
  `cancel_all_orders()` are verified to send signed REST requests against
  `POST /api/v3/order`, `DELETE /api/v3/order`, and
  `DELETE /api/v3/openOrders`, including symbol-rule, balance, and fee preflight
  routing. `amend_order()` remains explicitly unsupported for Spot and is
  covered at the unified capability/error boundary.
- MEXC Spot order-list support remains explicitly false because Spot OCO/OTO
  order lists are not mapped to a verified unified route; MEXC-specific
  order-list tests now prove shared market-type, symbol, leg price/stop-price,
  and client-order-id validation run before the explicit unsupported fallback.
- MEXC Spot private user-stream runtime remains explicitly unsupported with
  REST polling fallback, but parser-level fixtures now normalize
  representative JSON-envelope private order, fill, balance, and control/ack
  payloads into shared `UserStreamEvent` values without assuming Binance
  listenKey semantics.

Validation run:

```bash
cargo test mexc --lib
cargo test mexc_spot_client_should_route_common_private_rest_readbacks --lib
cargo test mexc_spot_client_should_route_order_mutations --lib
```

Result: 47 MEXC-filtered tests passed, 2 live-gated checks ignored in the
current workspace. The latest targeted Spot readback and mutation routing tests
also passed.

## KuCoin Spot Changes In This Pass

Implemented and tested in `src/exchanges/kucoin/mod.rs`:

- `place_order()` via signed `POST /api/v1/hf/orders`
- `cancel_order()` via signed `DELETE /api/v1/hf/orders/{orderId}` or
  `DELETE /api/v1/hf/orders/client-order/{clientOid}`, with client order id
  validation before the client-order URL is constructed.
- `cancel_all_orders()` via signed `DELETE /api/v1/hf/orders?symbol=...` for
  symbol-scoped requests and `DELETE /api/v1/hf/orders/cancelAll` for account
  scope. KuCoin HF responses only acknowledge request submission for these
  routes, so `cancelled_orders` remains `0` unless a response shape includes
  explicit order ids.
- `place_quote_market_order()` maps shared quote-sized market buys to signed
  `POST /api/v1/hf/orders` with `type=market` and `funds`, matching KuCoin HF
  market-order quote-funds semantics.
- `amend_order()` maps shared quantity amendments to signed
  `POST /api/v1/hf/orders/alter` with `newSize`, matching KuCoin HF's native
  cancel-replace modify endpoint. `new_client_order_id` remains explicitly
  unsupported because KuCoin documents `clientOid` only as the original-order
  selector, and is validated before returning the unsupported response.
  Dry-run coverage now also proves client-id-only amend selectors return a
  local ack with generated dry-run order id and the original `clientOid`.
- KuCoin signed REST transport now supports JSON request bodies.
- `subscribe_user_stream()` via signed `POST /api/v1/bullet-private`,
  subscribing to `/spotMarket/tradeOrders` and `/account/balance`.
- Symbol rules now advertise the limit-style order variants mapped by the
  adapter: post-only, IOC, and FOK.
- `TimeInForce::GTX` now maps to KuCoin HF `postOnly=true` so the shared
  post-only convention behaves consistently with other Spot adapters.
- `place_order()` now returns an offline submit ack in dry-run mode after local
  HF request-body validation.
- `cancel_order()` now returns an offline cancellation ack in dry-run mode and
  rejects non-Spot market types before signed REST deletion.
- `cancel_all_orders()` now returns an offline cancellation ack in dry-run mode
  and rejects non-Spot market types before signed REST deletion.
- KuCoin Spot private readbacks now have local HTTP mock routing coverage:
  `get_balances()`, `get_order()`, `get_open_orders()`, `get_fee_rate()`, and
  `get_recent_fills()` are verified to send signed REST requests with
  `KC-API-KEY`, `KC-API-SIGN`, `KC-API-TIMESTAMP`, signed
  `KC-API-PASSPHRASE`, `KC-API-KEY-VERSION=2`, `Content-Type`, and
  endpoint-specific query fields, then normalize KuCoin `data` and `items`
  response shapes into the shared `ExchangeClient` models. Open-order readback
  coverage also asserts client order id, side/status, price, quantity, filled
  quantity, and average-price absence so list parsing stays aligned with
  single-order parsing.
- KuCoin Spot private mutations now have local HTTP mock routing coverage:
  `place_order()`, `place_quote_market_order()`, `cancel_order()`,
  `cancel_all_orders()`, and `amend_order()` are verified against signed
  `POST /api/v1/hf/orders`, `DELETE /api/v1/hf/orders/{order_id}`,
  `DELETE /api/v1/hf/orders?symbol=...`, and
  `POST /api/v1/hf/orders/alter` requests. The test also covers quote-market
  preflight symbol-rule and balance readbacks and keeps
  `supports_order_list=false` explicit because KuCoin Spot OCO/OTO order lists
  are not mapped to a verified unified route; KuCoin-specific order-list tests
  now prove shared market-type, symbol, leg price/stop-price, and
  client-order-id validation run before the explicit unsupported fallback.

Validation run:

```bash
cargo test kucoin --lib
cargo test kucoin_spot_client_should_route_common_private_rest_readbacks --lib
cargo test kucoin_spot_client_should_route_order_mutations --lib
```

Result: 23 KuCoin-filtered lib tests passed in the current workspace. The
latest targeted Spot readback and mutation routing tests also passed.

## OKX Perpetual Changes In This Pass

Implemented and tested across `src/exchanges/okx/core.rs`,
`src/exchanges/private_perp/mod.rs`, `src/exchanges/registry.rs`, and
`src/exchanges/trading_adapters/mod.rs`:

- Signed GET calls in the legacy core path now include their query string in
  both the request path and OKX signature prehash for order query, open orders,
  order history, fills, and positions.
- Added request-path/body unit tests for legacy core order creation,
  cancel-all, leverage, position mode, and private WS login.
- Added `OkxPrivatePerpProtocol` on the side-effect-free `private_perp` path,
  covering order submit/query/cancel/cancel-all/batch-place/batch-cancel,
  all-orders history, fills-history, balances, positions, trade fee readback,
  leverage-info readback, amend, leverage, position mode, REST signing headers,
  private WS login/subscribe, and private WS order/fill/position/balance event
  parsing.
- `set_countdown_cancel_all()` maps to OKX `POST /api/v5/trade/cancel-all-after`
  with the documented `timeOut` body. Plain immediate cancel-all is composed
  from `GET /api/v5/trade/orders-pending` plus
  `POST /api/v5/trade/cancel-batch-orders` using each open order's `ordId` or
  `clOrdId`, matching OKX's batch-cancel requirement. Empty pending-order
  readbacks, and pending-order readbacks whose orders lack `ordId`/`clOrdId`,
  return zero-cancel acks without emitting batch-cancel.
- OKX symbol-scoped request specs now reject non-OKX command or exchange
  symbols before constructing order, batch, cancel, readback, fills, position,
  fee/config, leverage, amend, and countdown private REST requests.
- Registry routing now exposes OKX perpetual through `PrivatePerpExchange::Okx`
  and keeps the legacy `OkxExchange` core builder available separately for old
  callers.
- Adapter-level fills readback is now covered for OKX: `get_fills()` routes to
  `/api/v5/trade/fills-history` with `instType=SWAP`, symbol, order id, and
  limit query fields, and normalizes OKX fill envelopes into the shared
  `FillEvent`.
- Adapter-level positions and balances readbacks are now covered for OKX:
  `get_positions()` routes to `/api/v5/account/positions`,
  `get_balances()` routes to `/api/v5/account/balance`, and both normalize OKX
  response envelopes into shared position/balance snapshots.
- Adapter-level fee and symbol-account-config readbacks are now covered for
  OKX: `get_trade_fee()` routes to `/api/v5/account/trade-fee`,
  `get_symbol_account_config()` routes to `/api/v5/account/leverage-info`, and
  the shared config parser now handles OKX `mgnMode`.
- Adapter-level leverage setting is now covered for OKX: `set_leverage()`
  routes to `/api/v5/account/set-leverage`, preserves the requested leverage
  in the shared ack, and sends cross-margin `mgnMode`.
- Adapter-level position-mode routing is now covered for OKX:
  `set_position_mode()` emits `/api/v5/account/set-position-mode`, updates the
  adapter's local mode, and subsequent hedge-mode orders carry `posSide`.
- Adapter-level amend routing is now covered for OKX: `amend_order()` emits
  `/api/v5/trade/amend-order`, preserves `ordId`/`clOrdId` in the shared ack,
  and sends `newSz`/`newPx`.
- Adapter-level batch-place routing is now covered for OKX:
  `place_batch_orders()` emits `/api/v5/trade/batch-orders`, sends hedge-mode
  `posSide`, and splits OKX `sCode`/`sMsg` item results into shared accepted and
  failed order acknowledgements.
- Adapter-level batch-cancel routing is now covered for OKX:
  `cancel_batch_orders()` emits `/api/v5/trade/cancel-batch-orders`, preserves
  `ordId`/`clOrdId`, and maps per-item OKX `sCode`/`sMsg` results into
  cancelled or rejected shared cancel acknowledgements.
- Adapter-level close-position routing is now covered for OKX:
  `close_position()` emits a reduce-only `/api/v5/trade/order` market order,
  preserves `ordId` in the shared ack, and sends hedge-mode `posSide` for the
  closed side.
- Adapter-level countdown cancel-all routing is now covered for OKX:
  `set_countdown_cancel_all()` emits
  `/api/v5/trade/cancel-all-after`, preserves the requested `timeOut`, and
  normalizes the shared countdown ack message.
- Adapter-level all-orders history readback is now covered for OKX:
  `get_order_history()` routes to `/api/v5/trade/orders-history` with
  `instType=SWAP`, symbol, order id, and limit query fields, and normalizes OKX
  order-history envelopes into the shared `OrderState`.

Validation run:

```bash
cargo test okx_ --lib
cargo test okx_adapter_should_parse_order_history --lib
cargo test okx_adapter_should_route_fills_readback --lib
cargo test okx_adapter_should_route_positions_and_balances_readbacks --lib
cargo test okx_adapter_should_route_fee_and_symbol_account_config_readbacks --lib
cargo test okx_adapter_should_route_set_leverage --lib
cargo test okx_adapter_position_mode_should_route_and_update_local_state --lib
cargo test okx_adapter_should_route_amend_order --lib
cargo test okx_adapter_should_place_batch_orders_and_split_results --lib
cargo test okx_adapter_should_batch_cancel_and_split_item_results --lib
cargo test okx_adapter_should_route_close_position_as_reduce_only_order --lib
cargo test okx_adapter_should_route_countdown_cancel_all_after --lib
cargo test okx_adapter_order_amendment_history_should_fail_before_rest --lib
cargo test okx_adapter_cancel_all_should_skip_batch_cancel_when_no_open_orders --lib
cargo test okx_adapter_cancel_all_should_skip_batch_cancel_when_open_orders_lack_ids --lib
cargo test private_perp --lib
```

Result: 48 OKX-filtered lib tests passed, and the current private-perp suite
reports 187 passed tests after adding OKX protocol, composed cancel-all, OKX
symbol-scope validation, Gate/MEXC/HTX/Bitget symbol-scope validation, the MEXC
client-id batch-cancel fallback, Bybit cancel validation coverage, and
MEXC/HTX close-position, position-mode, fee/config, leverage, positions/balances,
fills readback adapter coverage, OKX/Bybit/Bitget adapter account/fills/leverage
readbacks, OKX/Bitget batch-place and batch-cancel adapter response splitting,
Binance/OKX/Bitget/Gate/Bybit/MEXC amend adapter routing,
OKX/Bitget/Gate/Bybit close-position and position-mode adapter routing, and
OKX/Bybit/Bitget/Gate countdown cancel-all adapter routing plus OKX
order-amendment-history adapter boundary coverage, HTX amend unsupported
adapter coverage, non-Binance order-amendment-history REST-prevention, empty
batch REST-prevention, Spot/shared orderbook depth validation, non-Binance
single-cancel adapter routing coverage, and OKX cancel-all empty-pending plus
missing-cancellable-id REST-prevention, with the subsequent MEXC composed
batch-cancel preflight validation and non-Binance single-order readback
plus open-order readback coverage and OKX all-orders history adapter readback
coverage included in the current suite count.

Additional OKX validation added in this pass:

- `cancel_order()` now validates OKX exchange-symbol scope and requires
  `ordId` or `clOrdId`.
- `cancel_batch_orders()` now rejects empty batches, batches over OKX's 20-order
  batch-cancel limit, non-OKX symbols, and items without `ordId` or `clOrdId`.
- `place_order()`, `place_batch_orders()`, query/open-order readbacks,
  fills-history, positions, trade-fee, leverage-info, leverage setting, amend,
  and cancel-all-after now share the same OKX symbol-scope boundary before
  request construction.
- The shared non-Binance order-amendment-history fallback now validates exchange
  scope and order identifiers before returning explicit unsupported.
- Adapter-level order-amendment-history boundary coverage now verifies OKX
  returns explicit unsupported before issuing REST and still rejects wrong
  exchange symbols at the adapter boundary.
- Request-spec tests cover these invalid order/readback/account/cancel
  boundaries while preserving the composed immediate cancel-all path that reads
  pending orders then submits `/api/v5/trade/cancel-batch-orders`.

## MEXC Perpetual Changes In This Pass

Implemented and tested in `src/exchanges/private_perp/mod.rs`:

- Native MEXC contract batch cancel still uses the documented order-id list
  shape when every cancel command has an exchange `orderId`.
- Adapter-level native batch-cancel routing is now covered for MEXC:
  `cancel_batch_orders()` emits `POST /api/v1/private/order/cancel` with an
  order-id array and splits MEXC `errorCode`/`errorMsg` item responses into
  shared cancelled/rejected acks.
- `PrivatePerpTradingAdapter::cancel_batch_orders()` now composes MEXC
  client-id batch cancel by issuing per-order `POST /api/v1/private/order/cancel`
  requests with `externalOid` and `symbol` when any command lacks an exchange
  order id. This preserves the shared batch-cancel contract for locally tracked
  orders whose exchange ids have not been reconciled yet. The composed fallback
  now validates every item has either `orderId` or `externalOid` before issuing
  any per-order REST request, preventing partial batch side effects.
- MEXC symbol-scoped request specs now validate MEXC exchange-symbol scope
  before constructing order, readback, fills, position, fee/config, amend, and
  leverage requests.
- MEXC amend requests now require at least one changed price or quantity field
  after validating exchange order id and symbol scope.
- The native MEXC batch-cancel protocol path now requires every batch item to
  have an exchange order id instead of silently dropping client-id-only items.
- Adapter-level amend routing is now covered for MEXC: `amend_order()` emits
  `/api/v1/private/order/change_order_price`, preserves the requested exchange
  order id in the shared ack, and sends price/volume changes.
- Adapter-level close-position routing is now covered for MEXC: shared
  `ClosePositionCommand` emits a reduce-only `POST /api/v1/private/order/submit`
  request, rejects wrong exchange-symbol scope before REST, and preserves MEXC
  `orderId` readback in the close ack.
- Adapter-level position-mode routing is now covered for MEXC:
  `set_position_mode()` emits `POST /api/v1/private/position/change_position_mode`,
  rejects wrong exchanges before REST, and updates local position mode so
  subsequent hedge-mode orders include MEXC `positionMode=hedge_mode`.
- Adapter-level fee and symbol-account-config readbacks are now covered for
  MEXC: `get_trade_fee()` routes to `/api/v1/private/account/tiered_fee_rate`,
  `get_symbol_account_config()` routes to `/api/v1/private/position/leverage`,
  and both normalize MEXC response envelopes into the shared snapshots.
- Adapter-level leverage setting is now covered for MEXC: `set_leverage()`
  routes to `/api/v1/private/position/change_leverage`, preserves the requested
  leverage in the shared ack, and rejects wrong exchange-symbol scope before
  REST.
- Adapter-level positions and balances readbacks are now covered for MEXC:
  `get_positions()` routes to `/api/v1/private/position/open_positions`,
  `get_balances()` routes to `/api/v1/private/account/assets`, and both
  normalize MEXC response envelopes into shared position/balance snapshots.
- Adapter-level fills readback is now covered for MEXC: `get_fills()` routes to
  `/api/v1/private/order/list/order_deals/{symbol}` with `order_id` and
  `page_size` query params, and normalizes MEXC fill envelopes into the shared
  `FillEvent`.
- Adapter-level countdown cancel-all capability gating is now covered for MEXC:
  valid MEXC symbols return the shared unsupported-capability error before any
  REST request is issued.

Validation run:

```bash
cargo test mexc_ --lib
cargo test mexc_adapter_should_route_close_position_as_reduce_only_order --lib
cargo test mexc_adapter_position_mode_should_route_and_update_local_state --lib
cargo test mexc_adapter_should_route_fee_and_symbol_account_config_readbacks --lib
cargo test mexc_adapter_should_route_set_leverage --lib
cargo test mexc_adapter_should_route_amend_order --lib
cargo test mexc_adapter_should_route_positions_and_balances_readbacks --lib
cargo test mexc_adapter_should_route_fills_readback --lib
cargo test mexc_adapter_should_batch_cancel_by_order_ids_and_split_errors --lib
cargo test mexc_and_htx_adapters_should_reject_close_scope_before_request --lib
cargo test mexc_and_htx_adapters_should_reject_leverage_scope_before_request --lib
cargo test mexc_and_htx_adapters_should_reject_position_mode_scope_before_request --lib
cargo test mexc_and_htx_adapters_should_reject_countdown_capability_before_rest --lib
cargo test mexc_adapter_should_reject_composed_batch_cancel_missing_identifiers_before_rest --lib
cargo test private_perp --lib
```

Result: 40 MEXC-filtered tests passed, 2 live-gated checks ignored, and the
current private-perp suite reports 187 passed tests after the subsequent OKX,
Gate, HTX, Bybit, Bitget, close-position, position-mode, fee/config readback,
leverage, positions/balances, fills readback, native batch-cancel adapter
routing, countdown capability-gating validation, and MEXC composed batch-cancel
preflight validation plus non-Binance single-order readback coverage passes.
The current suite count also includes non-Binance open-order readback coverage
and OKX all-orders history adapter readback coverage.

## Bitget Perpetual Changes In This Pass

Implemented and tested in `src/exchanges/private_perp/mod.rs`:

- Symbol-scoped Bitget mix/UTA request specs now reject non-Bitget command or
  exchange symbols before constructing order, batch, cancel-all, readback,
  fills, position, fee/config, leverage, and countdown requests.
- `cancel_order()` requires either `orderId` or `clientOid`.
- `cancel_batch_orders()` now rejects empty batches, batches over Bitget's
  50-order limit, non-Bitget symbols, non-Bitget batch commands, and items
  without `orderId` or `clientOid`.
- `amend_order()` now validates Bitget exchange-symbol scope before constructing
  Bitget mix-order modify payloads.
- Request-spec tests now cover these invalid order/readback/account/cancel and
  amend boundaries, preventing incomplete Bitget mix/UTA payloads from reaching
  REST.
- Adapter-level fills readback is now covered for Bitget: `get_fills()` routes
  to `/api/v2/mix/order/fills` with `productType`, symbol, order id, and limit
  query fields, and normalizes Bitget fill envelopes into the shared
  `FillEvent`.
- Adapter-level positions and balances readbacks are now covered for Bitget:
  `get_positions()` routes to `/api/v2/mix/position/all-position`,
  `get_balances()` routes to `/api/v2/mix/account/accounts`, and both normalize
  Bitget response envelopes into shared position/balance snapshots.
- Adapter-level fee and symbol-account-config readbacks are now covered for
  Bitget: `get_trade_fee()` routes to `/api/v2/mix/market/contracts`,
  `get_symbol_account_config()` routes to `/api/v2/mix/account/account`, and
  both normalize Bitget readbacks into shared snapshots.
- Adapter-level leverage setting is now covered for Bitget: `set_leverage()`
  routes to `/api/v2/mix/account/set-leverage`, preserves the requested
  leverage in the shared ack, and sends product type, symbol, and margin coin.
- Adapter-level position-mode routing is now covered for Bitget:
  `set_position_mode()` emits `/api/v2/mix/account/set-position-mode`, updates
  the adapter's local mode, and subsequent hedge-mode orders carry
  `tradeSide=open` without one-way `reduceOnly`.
- Adapter-level amend routing is now covered for Bitget: `amend_order()` emits
  `/api/v2/mix/order/modify-order`, preserves exchange/client order ids in the
  shared ack, and sends `newSize`, `newPrice`, and optional `newClientOid`.
- Adapter-level batch-cancel routing is now covered for Bitget:
  `cancel_batch_orders()` emits `/api/v2/mix/order/batch-cancel-orders`,
  preserves `orderId`/`clientOid`, and splits Bitget `successList` and
  `failureList` response envelopes into shared cancelled/rejected
  acknowledgements.
- Adapter-level close-position routing is now covered for Bitget:
  `close_position()` emits a hedge-mode close order through
  `/api/v2/mix/order/place-order`, preserves the exchange order id in the shared
  ack, and carries Bitget `tradeSide=close` without relying on `reduceOnly` in
  hedge mode.

Validation run:

```bash
cargo test bitget --lib
cargo test bitget_adapter_should_route_fills_readback --lib
cargo test bitget_adapter_should_route_positions_and_balances_readbacks --lib
cargo test bitget_adapter_should_route_fee_and_symbol_account_config_readbacks --lib
cargo test bitget_adapter_should_route_set_leverage --lib
cargo test bitget_adapter_position_mode_should_route_and_update_local_state --lib
cargo test bitget_adapter_should_route_amend_order --lib
cargo test bitget_adapter_should_batch_cancel_and_split_item_results --lib
cargo test bitget_adapter_should_route_close_position_as_hedge_close_order --lib
cargo test private_perp --lib
```

Result: 47 Bitget-filtered tests passed, and the current private-perp suite
reports 177 passed tests after the subsequent OKX, Gate, MEXC, HTX, Bybit,
Binance, close-position, position-mode, fee/config readback, leverage,
positions/balances, and fills readback validation passes.

## Bybit Perpetual Changes In This Pass

Implemented and tested in `src/exchanges/private_perp/mod.rs`:

- Bybit symbol-scoped request specs now reject non-Bybit command or exchange
  symbols before constructing order, batch, cancel-all, readback, fills,
  position, fee/config, leverage, amend, and disconnected cancel-all requests.
- Single-order readback now requires `orderId` or `orderLinkId`, matching the
  shared one-order query contract used by Binance/OKX/Bitget/Gate/MEXC/HTX.
- `cancel_order()` requires either `orderId` or `orderLinkId`.
- `cancel_batch_orders()` now rejects empty batches, batches over Bybit's
  linear 20-order limit, non-Bybit symbols, non-Bybit batch commands, and items
  without `orderId` or `orderLinkId`.
- `amend_order()` now rejects non-Bybit symbols, missing `orderId`/`orderLinkId`,
  missing changed price/quantity fields, and unsupported `new_client_order_id`
  instead of silently dropping unsupported shared amend fields.
- Request-spec tests now cover these invalid order/readback/account/batch-cancel
  and amend boundaries so shared commands cannot silently emit incomplete Bybit
  V5 items.
- Adapter-level fills readback is now covered for Bybit: `get_fills()` routes to
  `/v5/execution/list` with `category=linear`, symbol, order id, client order
  id, and limit query fields, and normalizes Bybit execution envelopes into the
  shared `FillEvent`.
- Adapter-level positions and balances readbacks are now covered for Bybit:
  `get_positions()` routes to `/v5/position/list`, `get_balances()` routes to
  `/v5/account/wallet-balance`, and both normalize Bybit V5 `result.list`
  envelopes into shared position/balance snapshots.
- Adapter-level fee and symbol-account-config readbacks are now covered for
  Bybit: `get_trade_fee()` routes to `/v5/account/fee-rate`,
  `get_symbol_account_config()` routes through `/v5/position/list`, and the
  shared fee/config parser now handles nested `result.list` readback envelopes.
- Adapter-level leverage setting is now covered for Bybit: `set_leverage()`
  routes to `/v5/position/set-leverage`, preserves the requested leverage in
  the shared ack, and sends matching buy/sell leverage values.
- Adapter-level position-mode routing is now covered for Bybit:
  `set_position_mode()` emits `/v5/position/switch-mode`, updates the adapter's
  local mode, and subsequent hedge-mode orders carry `positionIdx`.
- Adapter-level amend routing is now covered for Bybit: `amend_order()` emits
  `/v5/order/amend`, preserves `orderId`/`orderLinkId` in the shared ack, and
  sends changed `qty`/`price`.
- Adapter-level batch-cancel routing is now covered for Bybit:
  `cancel_batch_orders()` emits `/v5/order/cancel-batch`, preserves
  `orderId`/`orderLinkId`, and splits Bybit `result.list` plus
  `retExtInfo.list` into shared cancelled/rejected acknowledgements.
- Adapter-level close-position routing is now covered for Bybit:
  `close_position()` emits a reduce-only `/v5/order/create` market order,
  preserves the exchange order id in the shared ack, and sends hedge-mode
  `positionIdx` for the closed side.
- Adapter-level disconnected countdown cancel-all routing is now covered for
  Bybit: `set_countdown_cancel_all()` emits
  `/v5/order/disconnected-cancel-all`, preserves the requested `timeWindow`,
  and normalizes Bybit `retMsg` into the shared countdown ack message.

Validation run:

```bash
cargo test bybit_ --lib
cargo test bybit_adapter_should_route_fills_readback --lib
cargo test bybit_adapter_should_route_positions_and_balances_readbacks --lib
cargo test bybit_adapter_should_route_fee_and_symbol_account_config_readbacks --lib
cargo test bybit_adapter_should_route_set_leverage --lib
cargo test bybit_adapter_position_mode_should_route_and_update_local_state --lib
cargo test bybit_adapter_should_route_amend_order --lib
cargo test bybit_adapter_should_batch_cancel_and_split_ret_ext_info --lib
cargo test bybit_adapter_should_route_close_position_as_reduce_only_order --lib
cargo test bybit_adapter_should_route_disconnected_countdown_cancel_all --lib
cargo test private_perp --lib
```

Result: 18 Bybit-filtered tests passed, and the current private-perp suite
reports 177 passed tests after the subsequent Bitget/OKX/Gate/MEXC/HTX/Binance
symbol-scope, close-position, position-mode, fee/config readback, leverage,
positions/balances, and fills readback validation passes.

## HTX Perpetual Changes In This Pass

Implemented and tested in `src/exchanges/private_perp/mod.rs`:

- `cancel_order()` now validates HTX exchange-symbol scope and requires either
  `order_id` or `client_order_id`, avoiding malformed cancel requests that only
  contain `contract_code`.
- `cancel_batch_orders()` now validates that every batch item is HTX, shares
  one `contract_code`, and has either `order_id` or `client_order_id`.
- `get_order()` now requires `order_id` or `client_order_id` before building
  HTX `swap_cross_order_info`, matching the shared single-order readback
  contract.
- Batch cancel request-spec tests now cover both exchange-order-id lists and
  HTX's hashed `client_order_id` list shape.
- Adapter-level batch-cancel routing is now covered for HTX:
  `cancel_batch_orders()` emits `/linear-swap-api/v1/swap_cross_cancel`,
  sends comma-joined `order_id` values for one `contract_code`, and splits HTX
  `data.success` plus `data.errors` into shared cancelled/rejected acks.
- HTX symbol-scoped request specs now validate HTX exchange-symbol scope before
  constructing order, cancel-all, readback, fills, position, fee/config,
  amend, and leverage requests.
- `amend_order()` remains explicitly unsupported because no verified HTX
  linear-swap amend endpoint is mapped, but the request-spec path now validates
  order identifiers and changed price/quantity fields before returning
  unsupported.
- Adapter-level amend unsupported-boundary coverage is now in place for HTX:
  valid amend commands return the explicit unsupported error before REST and
  leave the transport untouched.
- Adapter-level close-position routing is now covered for HTX: market close
  uses native cross lightning close, wrong exchange-symbol scope is rejected
  before REST, and close acks now read HTX `data.order_id_str`/`order_id`
  instead of falling back to the client id.
- Adapter-level position-mode routing is now covered for HTX:
  `set_position_mode()` emits
  `POST /linear-swap-api/v1/swap_cross_switch_position_mode`, rejects wrong
  exchanges before REST, and updates local position mode before subsequent
  order routing.
- Adapter-level fee and symbol-account-config readbacks are now covered for
  HTX: `get_trade_fee()` routes to `/linear-swap-api/v1/swap_fee`,
  `get_symbol_account_config()` routes through
  `/linear-swap-api/v1/swap_cross_position_info`, and both normalize HTX
  response envelopes into the shared snapshots.
- Adapter-level leverage setting is now covered for HTX: `set_leverage()`
  routes to `/linear-swap-api/v1/swap_cross_switch_lever_rate`, preserves the
  requested leverage in the shared ack, and rejects wrong exchange-symbol scope
  before REST.
- Adapter-level positions and balances readbacks are now covered for HTX:
  `get_positions()` routes to `/linear-swap-api/v1/swap_cross_position_info`,
  `get_balances()` routes to `/linear-swap-api/v1/swap_cross_account_info`,
  and both normalize HTX response envelopes into shared position/balance
  snapshots.
- Adapter-level fills readback is now covered for HTX: `get_fills()` routes to
  `/linear-swap-api/v1/swap_cross_matchresults` with `contract_code` and
  `page_size` body fields, and normalizes HTX fill envelopes into the shared
  `FillEvent`.
- Adapter-level countdown cancel-all capability gating is now covered for HTX:
  valid HTX symbols return the shared unsupported-capability error before any
  REST request is issued.

Validation run:

```bash
cargo test htx_ --lib
cargo test htx_adapter_should_route_market_close_to_lightning_close --lib
cargo test htx_adapter_position_mode_should_route_and_update_local_state --lib
cargo test htx_adapter_should_route_fee_and_symbol_account_config_readbacks --lib
cargo test htx_adapter_should_route_set_leverage --lib
cargo test htx_adapter_should_route_positions_and_balances_readbacks --lib
cargo test htx_adapter_should_route_fills_readback --lib
cargo test htx_adapter_should_batch_cancel_and_split_indexed_results --lib
cargo test htx_adapter_should_reject_amend_unsupported_before_rest --lib
cargo test mexc_and_htx_adapters_should_reject_close_scope_before_request --lib
cargo test mexc_and_htx_adapters_should_reject_leverage_scope_before_request --lib
cargo test mexc_and_htx_adapters_should_reject_position_mode_scope_before_request --lib
cargo test mexc_and_htx_adapters_should_reject_countdown_capability_before_rest --lib
cargo test private_perp --lib
```

Result: 24 HTX-filtered tests passed, and `cargo test private_perp --lib`
reports 179 passed tests after the subsequent Bybit/Bitget/OKX
cancel-validation, Gate/MEXC/HTX symbol-scope validation, close-position, and
position-mode adapter routing, fee/config readback, leverage, and
positions/balances plus fills readback and HTX batch-cancel adapter routing
plus countdown capability-gating, amend unsupported-boundary, and non-Binance
order-amendment-history REST-prevention passes.

## Gate Perpetual Changes In This Pass

Implemented and tested in `src/exchanges/private_perp/mod.rs`:

- Added a shared Gate symbol-scope guard for symbol-scoped REST request specs.
- `place_order`, `cancel_order`, `cancel_all_orders`, `get_order`,
  `get_open_orders`, `get_fills`, `get_positions`, `get_trade_fee`,
  `get_symbol_account_config`, `amend_order`, and `set_leverage` now reject
  non-Gate `ExchangeSymbol` inputs before constructing REST paths, queries, or
  bodies.
- Request-spec tests cover the invalid symbol-scope boundary across the Gate
  order, query, fill, position, fee/config, amend, and leverage paths.
- Adapter-level balances, fee, and symbol-account-config readbacks are now
  covered for Gate: `get_balances()` routes to `/futures/usdt/accounts`,
  `get_trade_fee()` routes to `/futures/usdt/contracts/{contract}`,
  `get_symbol_account_config()` routes to `/futures/usdt/positions/{contract}`,
  and all normalize Gate response envelopes into shared snapshots.
- Adapter-level leverage setting is now covered for Gate: `set_leverage()`
  routes to `/futures/usdt/positions/{contract}/leverage`, preserves the
  requested leverage in the shared ack, and validates Gate symbol scope before
  REST.
- Adapter-level amend routing is now covered for Gate: `amend_order()` emits
  `PATCH /futures/usdt/orders/{id}`, keeps Gate text client ids stable, and
  sends price plus optional `amend_text`.
- Adapter-level close-position routing is now covered for Gate:
  `close_position()` emits a reduce-only `/futures/usdt/orders` market order,
  preserves the exchange order id in the shared ack, and keeps the Gate text
  client id prefix stable.
- Adapter-level position-mode capability gating is now covered for Gate:
  valid Gate position-mode requests return the shared unsupported-capability
  error before any REST request is issued.

Validation run:

```bash
cargo test gate --lib
cargo test gate_adapter_should_route_balances_and_fee_config_readbacks --lib
cargo test gate_adapter_should_route_set_leverage --lib
cargo test gate_adapter_should_route_amend_order --lib
cargo test gate_adapter_should_route_close_position_as_reduce_only_order --lib
cargo test gate_adapter_should_reject_position_mode_capability_before_rest --lib
cargo test private_perp --lib
```

Result: 67 Gate-filtered tests passed, 2 live-gated checks ignored, and the
current private-perp suite reports 177 passed tests after the subsequent
MEXC/HTX symbol-scope validation, shared history/batch/position-mode
validation, OKX/Bitget/Gate/Bybit/MEXC/HTX close-position adapter routing, and
OKX/Bitget/Bybit/MEXC/HTX position-mode adapter routing plus Gate/MEXC/HTX
fee/config readback, leverage, positions/balances, fills readback, and Gate
position-mode capability-gating passes.

## USDT Perpetual Coverage

| Exchange | Path | Unified completion | Status |
| --- | --- | ---: | --- |
| Binance | `src/exchanges/private_perp/mod.rs`, `src/exchanges/binance/core.rs`, `src/exchanges/trading_adapters/mod.rs`, `src/exchanges/market_adapters/binance.rs` | 90-95% | Now aligned with the side-effect-free `private_perp` protocol and still retains the legacy core/gateway path. Covers market data, order submit/query/cancel/cancel-all/batch-place/batch-cancel/amend adapter routing, single-cancel and single/open-order readback adapter routing, native cancel-all adapter routing, all-orders history, order-amendment history, user trades/fills adapter routing, balances/positions adapter routing, fee/config readback adapter routing, leverage adapter routing, position-mode adapter routing, countdown cancel-all, request signing, native listenKey create/keepalive in the new private WS runtime, pre-created listenKey compatibility, user-stream event parsing, and symbol-scoped `/fapi` request validation. The remaining gaps are beyond the current common 90% baseline: live exchange-validation depth and optional niche endpoints. |
| OKX | `src/exchanges/private_perp/mod.rs`, `src/exchanges/okx/core.rs`, `src/exchanges/registry.rs` | 85-90% | Now registered on the side-effect-free `private_perp` protocol while retaining the legacy core path. Covers order submit/query/cancel/cancel-all/batch-place/batch-cancel, ordinary place-order adapter routing, all-orders history with adapter routing, fills-history, balances, positions, trade fee readback, leverage-info readback, close-position routing, single-order and open-order readback adapter routing by `ordId`/`clOrdId`, batch-place adapter routing, batch-cancel adapter routing, single-cancel adapter routing by `ordId`/`clOrdId`, amend adapter routing, leverage adapter routing, position-mode adapter routing, countdown cancel-all via OKX Cancel All After with adapter routing, REST signing headers including passphrase and simulated-trading flag, private WS login/subscribe endpoint construction, and private WS order/fill/position/balance/control parser coverage. Symbol-scoped order, batch, cancel, readback, account, leverage, amend, and countdown request specs now validate OKX scope plus identifier requirements where applicable, including batch-cancel command exchange and amend id/change validation before unsupported client-id replacement. Order-amendment history remains an explicit unsupported fallback after scope/id validation because no OKX equivalent is mapped. Immediate cancel-all is composed from pending-order readback plus batch-cancel by `ordId`/`clOrdId` because OKX has no single ordinary order-book cancel-all endpoint; the empty pending-order branch returns a zero-cancel ack after readback without sending batch-cancel. Remaining gap: live validation depth comparable to Binance/Bitget/Gate. |
| Bitget | `src/exchanges/private_perp/mod.rs` | 85-90% | Newer protocol path: request specs, signing, private WS order/fill/position/balance/control parser coverage, ordinary place-order adapter routing, all-orders history, fills, positions, balances, fee/config, leverage adapter routing, position-mode adapter routing, close-position routing, amend adapter routing, single-order and open-order readback adapter routing by `orderId`/`clientOid`, single-cancel adapter routing by `orderId`/`clientOid`, native cancel-all adapter routing, batch-place and batch-cancel adapter routing, validated single/batch cancel and amend by `orderId` or `clientOid`, symbol-scoped validation across order/batch/cancel/readback/account/leverage/countdown paths, all-orders/fills time-window ordering validation before request construction, and UTA countdown cancel-all. Order-amendment history remains an explicit unsupported fallback and is now covered at the adapter layer before REST. Countdown cancel-all is only available for Bitget UTA accounts, not classic mix accounts. |
| Gate | `src/exchanges/private_perp/mod.rs` | 85-90% | Newer protocol path with Gate contract-size normalization, ordinary place-order adapter routing, all-orders history, REST batch-place with command/item scope and batch-size validation, native single-order/open-order readback and single-cancel adapter routing by order id/client text, native cancel-all adapter routing, native batch cancel by ID list with command/item scope validation, balances, fee/config readback, leverage, close-position routing, amend adapter routing, symbol-scope validation across order/query/fill/position/fee/config/amend/leverage/countdown paths including query/command exchange checks, countdown cancel-all, and private WS order/fill/position/balance/control parser coverage. Open-order readback coverage now asserts the normalized client id, side, position side, order type, status, quantity, filled quantity, and price after contract-size conversion. Order-amendment history remains an explicit unsupported fallback and is now covered at the adapter layer before REST. Size amend requires original side; position-mode change remains unsupported after exchange-scope validation and is rejected by the adapter capability gate before REST. |
| Bybit | `src/exchanges/private_perp/mod.rs` | 85-90% | Newer protocol path with REST signing/private WS, request-spec coverage, common order/account routes, ordinary place-order adapter routing, all-orders history, fills, positions, balances, fee/config readback, close-position routing, batch-place, native cancel-all and batch-cancel adapter routing, validated single-order and open-order readbacks, single-cancel adapter routing by `orderId`/`orderLinkId`, single/batch cancel, amend adapter routing by `orderId` or `orderLinkId`, amend changed-field validation, symbol-scoped validation across order/batch/cancel/readback/account/leverage/countdown paths, leverage adapter routing, position-mode adapter routing, disconnected cancel-all adapter routing, and private WS order/fill/position/balance/control parser coverage. Order-amendment history remains an explicit unsupported fallback and is now covered at the adapter layer before REST. Needs live validation depth comparable to Bitget/Gate. |
| MEXC | `src/exchanges/private_perp/mod.rs` | 85-90% | Newer protocol path with common order/account routes, ordinary place-order adapter routing, all-orders history, fills, positions, balances, fee/config readback, leverage, position mode, close-position routing, amend adapter routing, single-order/open-order readback and single-cancel adapter routing by `orderId`/`externalOid`, native cancel-all adapter routing, batch-place request/response splitting for the official maintenance-gated endpoint with command/item scope and batch-size validation, native batch-cancel adapter routing by exchange order-id list plus composed `externalOid` batch-cancel fallback with command/item scope validation, symbol-scope validation across order/readback/fill/position/fee/config/amend/leverage/countdown paths, amend identifier and changed-field validation, request-spec coverage, and private WS order/fill/position/balance/control parser coverage. Order-amendment history remains an explicit unsupported fallback and is now covered at the adapter layer before REST. Countdown cancel-all validates scope before explicit unsupported because the documented contract API exposes ordinary cancel-all but no Binance-like countdown cancel-all. Live validation depth should be increased. |
| HTX | `src/exchanges/private_perp/mod.rs` | 85-90% | Newer protocol path with common reads/orders, ordinary place-order adapter routing, all-orders history, fills, positions, balances, fee/config readback, leverage, cross position-mode switching/readback, batch-place with command/item scope and batch-size validation, single-cancel adapter routing by order id/hashed client order id, native cancel-all adapter routing, batch-cancel adapter routing with command/item scope validation, request-spec coverage, validated single-order and open-order readbacks plus single/batch cancel by exchange order id or hashed client order id, symbol-scope validation across order/cancel-all/readback/fill/position/fee/config/amend/leverage/countdown paths including exchange-command scope for global cancel-all, private WS order/fill/position/balance/control parser coverage, and native cross lightning close for market close-position commands. Amend validates identifier and changed-field requirements before explicit unsupported, with adapter coverage proving no REST request is emitted; order-amendment history remains an explicit unsupported fallback and is now covered at the adapter layer before REST; countdown cancel-all validates scope before explicit unsupported because no verified HTX linear-swap endpoint matches the Binance countdown semantics. |
| CoinEx | `src/exchanges/coinex/mod.rs`, `src/exchanges/private_perp/mod.rs`, `src/exchanges/registry.rs` | 10-15% | Spot is complete separately, and a staged `CoinExPrivatePerpProtocol` offline skeleton now exists for request-spec construction, v2 REST signing headers, WS `server.sign` shape, order/batch/cancel/readback/balance/position/leverage/close/amend route specs, and explicit unsupported boundaries for position-mode change and countdown cancel-all. The registry intentionally still returns `None` for CoinEx private-perp gateways, so no live/private REST execution path is enabled yet. Remaining work before raising completion: response parsers, adapter routing with mock transport, private WS event parsing, market adapter/symbol metadata, capability matrix inclusion, and live readonly validation. |
| KuCoin | `src/exchanges/kucoin/mod.rs`, `src/exchanges/registry.rs` | 0% | Spot-only in this workspace. No `PrivatePerpExchange::KuCoin`, no `KuCoinPrivatePerpProtocol`, and the registry intentionally returns `None` for KuCoin private-perp gateways. Treat KuCoin USDT perpetual as a scope gap requiring KuCoin Futures API research plus new protocol/factory/registry/capability-matrix tests before claiming Binance USD-M parity. |

### CoinEx/KuCoin USDT Perpetual Implementation Recon

Official docs show that the staged CoinEx row and the remaining 0% KuCoin row
are implementable, but they should be added as new `private_perp` protocols
rather than stretching the existing Spot clients.

| Exchange | Official API evidence | Minimal `private_perp` mapping | Notes before coding |
| --- | --- | --- | --- |
| CoinEx | v2 HTTP base `https://api.coinex.com/v2`, futures WS `wss://socket.coinex.com/v2/futures`; futures modules include market, order, position, and assets. Key endpoints: `POST /futures/order`, `POST /futures/batch-order`, `DELETE /futures/order`, `POST /futures/cancel-all-order`, `POST /futures/cancel-batch-order`, `GET /futures/order-status`, `GET /futures/pending-order`, `GET /futures/finished-order`, `GET /futures/user-deals`, `GET /futures/pending-position`, `GET /assets/futures/balance`, `POST /futures/modify-order`, `POST /futures/adjust-position-leverage`, `POST /futures/close-position`. | Add `PrivatePerpExchange::CoinEx`, `CoinExPrivatePerpProtocol`, CoinEx REST signing headers, request specs for order/batch/cancel/cancel-all/batch-cancel/readbacks/balances/positions/leverage/close-position/amend, and WS `server.sign` plus order/deal/position parser coverage. | Symbol format appears Spot-like (`BTCUSDT`) with `market_type=FUTURES`; order requests use `amount`, optional `price`, and `client_id`. Start with `supports_position_mode_change=false` and `supports_countdown_cancel_all=false` because no verified Binance-equivalent endpoints were found. |
| KuCoin | Futures REST domain is `https://api-futures.kucoin.com`; Futures private WS token is separate from Spot and uses Futures WS domains. Key endpoints shown in official docs include `POST /api/v1/orders`, futures batch add orders, order-id/client-id cancel routes, `DELETE /api/v3/orders` cancel-all, `GET /api/v1/orders`, `GET /api/v1/orders/{orderId}`, fills/history routes, futures account overview, position, contract, and leverage/margin routes. | Add `PrivatePerpExchange::KuCoin`, `KuCoinPrivatePerpProtocol`, KuCoin Futures REST signing using key/secret/passphrase, request specs for order/batch/cancel/cancel-all/readbacks/fills/balances/positions/fee/leverage where verified, and private Futures token/channel parser coverage. | Futures symbols use contract names like `XBTUSDTM`; order size is integer contract `size`, so instrument metadata must drive base-to-contract conversion like Gate. Start with `supports_amend_order=false`, `supports_position_mode_change=false`, and `supports_countdown_cancel_all=false` until native modify, hedge-mode, and DCP/dead-man-cancel semantics are verified. |

Minimum safe coding sequence:

1. Add enum/factory/registry skeletons with capabilities set conservatively.
2. Add request-spec tests before any transport execution path is registered.
3. Add parser tests for REST envelopes, then adapter routing tests with mock
   transport.
4. Only after offline routing passes, enable registry gateway rows and update
   capability matrices from 0% to partial completion.

## Binance USD-M 90% Target Checklist

Binance is now the benchmark for other perpetual tasks through
`BinancePrivatePerpProtocol` in `src/exchanges/private_perp/mod.rs`, while the
legacy `BinanceExchange` core path remains available for existing callers.

Current unified/private-perp `/fapi` surface:

- Market: `/fapi/v1/exchangeInfo`, `/fapi/v1/depth`, `/fapi/v1/klines`,
  `/fapi/v1/ticker/24hr`, `/fapi/v1/ticker/bookTicker`,
  `/fapi/v1/premiumIndex`, `/fapi/v1/fundingRate`
- Orders: `POST/GET/DELETE/PUT /fapi/v1/order`,
  `GET /fapi/v1/openOrders`, `DELETE /fapi/v1/allOpenOrders`,
  `GET /fapi/v1/allOrders`, `GET /fapi/v1/orderAmendment`,
  `POST/DELETE /fapi/v1/batchOrders`
- Account: `GET /fapi/v2/account`, `GET /fapi/v2/balance`,
  `GET /fapi/v2/positionRisk`, `GET /fapi/v1/userTrades`,
  `GET/POST /fapi/v1/positionSide/dual`, `POST /fapi/v1/leverage`
- Safety: `POST /fapi/v1/countdownCancelAll`
- Streams: `POST/PUT /fapi/v1/listenKey`, private WS normalization for
  `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`, listen-key expiry, and disconnect
- Request boundary: USD-M symbol-scoped order, batch, readback, account,
  leverage, and countdown specs reject non-Binance command or exchange symbols
  before constructing `/fapi` requests.
- Adapter routing: batch-cancel now has direct coverage for
  `cancel_batch_orders()` issuing `DELETE /fapi/v1/batchOrders` with
  `orderIdList` and normalizing Binance item responses into shared cancel
  acknowledgements.
- Adapter routing: single cancel/query/open-orders now have direct coverage for
  `DELETE /fapi/v1/order`, `GET /fapi/v1/order`, and
  `GET /fapi/v1/openOrders`, including shared `CancelAck` and `OrderState`
  normalization.
- Adapter routing: native cancel-all now has direct coverage for Binance,
  Bitget, Gate, Bybit, MEXC, and HTX request routing plus shared
  `CancelAllAck` count/message normalization.
- Adapter routing: account and fill readbacks now have direct Binance coverage
  for `GET /fapi/v1/userTrades`, `GET /fapi/v2/positionRisk`,
  `GET /fapi/v2/balance`, `GET /fapi/v1/commissionRate`, and
  `GET /fapi/v1/symbolConfig`, including shared `FillEvent`,
  `ExchangePosition`, `ExchangeBalance`, `TradeFeeSnapshot`, and
  `SymbolAccountConfig` normalization.

Validation run:

```bash
cargo test binance_ --lib
cargo test binance_adapter_should_batch_cancel_by_order_ids --lib
cargo test binance_adapter_should_route_single_cancel_and_order_readbacks --lib
cargo test binance_adapter_should_route_account_and_fill_readbacks --lib
cargo test binance_adapter_should_route_amend_order --lib
cargo test binance_adapter_should_route_set_leverage --lib
cargo test binance_adapter_position_mode_should_route_and_update_local_state --lib
cargo test private_perp_trading_adapter_should_ack_empty_batches_without_rest --lib
cargo test okx_adapter_cancel_all_should_skip_batch_cancel_when_no_open_orders --lib
cargo test non_binance_adapters_should_route_single_cancel_requests --lib
cargo test mexc_adapter_should_reject_composed_batch_cancel_missing_identifiers_before_rest --lib
cargo test non_binance_adapters_should_route_single_order_readbacks --lib
cargo test non_binance_adapters_should_route_open_order_readbacks --lib
cargo test okx_adapter_should_parse_order_history --lib
cargo test native_cancel_all_adapters_should_route_requests --lib
cargo test private_perp --lib
```

Result: `cargo test binance_ --lib` reports 52 passed tests, and
`cargo test private_perp --lib` reports 198 passed tests in the current
workspace after the subsequent HTX amend unsupported-boundary, non-Binance
order-amendment-history REST-prevention, empty batch REST-prevention, and
Spot/shared orderbook depth validation, non-Binance single-cancel adapter
routing coverage, and OKX cancel-all empty-pending plus missing-cancellable-id
REST-prevention plus MEXC composed batch-cancel preflight and non-Binance
single-order/open-order readback additions plus OKX all-orders history adapter
readback coverage, Binance single-cancel/query/open-orders adapter coverage,
and native cancel-all adapter routing coverage for Binance, Bitget, Gate,
Bybit, MEXC, and HTX plus Binance account/fill readback adapter coverage.

Latest incremental validation:
`cargo test non_binance_adapters_should_route_single_cancel_requests --lib`
passed after adding non-Binance single-cancel adapter routing coverage.
`cargo test okx_adapter_cancel_all_should_skip_batch_cancel_when_open_orders_lack_ids --lib`
passed after adding OKX missing-cancellable-id cancel-all coverage, and
`cargo test mexc_adapter_should_reject_composed_batch_cancel_missing_identifiers_before_rest --lib`
passed after adding MEXC composed batch-cancel preflight coverage.
`cargo test non_binance_adapters_should_route_single_order_readbacks --lib`
passed after adding non-Binance single-order readback adapter coverage and
readback envelope normalization. `cargo test
non_binance_adapters_should_route_open_order_readbacks --lib` passed after
adding non-Binance open-order readback adapter coverage. `cargo test
okx_adapter_should_parse_order_history --lib` passed after adding OKX
all-orders history adapter readback coverage. `cargo test
binance_adapter_should_route_single_cancel_and_order_readbacks --lib` passed
after adding Binance single-cancel/query/open-orders adapter coverage.
`cargo test native_cancel_all_adapters_should_route_requests --lib` passed
after adding native cancel-all adapter coverage for Binance, Bitget, Gate,
Bybit, MEXC, and HTX. `cargo test
binance_adapter_should_route_account_and_fill_readbacks --lib` passed after
adding Binance fills/positions/balances/fee/config adapter readback coverage.
`cargo test non_binance_adapters_should_route_account_and_fill_readbacks --lib`
passed after adding the matching OKX/Bitget/Gate/Bybit/MEXC/HTX
fills/positions/balances/fee/config adapter readback aggregate coverage, and
now asserts trade ids, order ids, client ids, side/position side, liquidity,
price, quantity, quote quantity, fees, realized PnL, position marks, balances,
fee rates, leverage, margin mode, and position mode across those venue
envelopes.
`cargo test non_binance_adapters_should_parse_order_history --lib` passed
after adding OKX/Bitget/Gate/Bybit/MEXC/HTX all-orders history aggregate
routing and normalization coverage, and now asserts client order id,
side/position side, order type, status, quantity, filled quantity, price, and
average fill price across those venue envelopes. `cargo test
non_binance_adapters_should_route_single_order_readbacks --lib` passed after
aligning Bitget and MEXC single-order readback normalization so live/open
orders with nonzero executed quantity surface as shared `PartiallyFilled`
orders. `cargo test
okx_private_ws_endpoint_should_login_and_subscribe_all_private_channels --lib`
passed after adding OKX private WS login plus orders/positions/account
subscribe endpoint coverage. `cargo test okx_should_parse_private_events
--lib` passed after expanding OKX private WS parser fixtures to include
subscribe ack, login error, and event error control coverage. `cargo test
reqwest_transport_should_build_okx_headers_with_passphrase_and_demo_flag --lib`
and `cargo test reqwest_transport_should_reject_okx_headers_without_passphrase
--lib` passed after adding OKX private REST transport header coverage for
passphrase, simulated-trading, timestamp, signature, and missing-passphrase
validation. `cargo test
non_binance_adapters_should_route_place_order_requests --lib` passed after
adding OKX/Bitget/Gate/Bybit/MEXC/HTX ordinary place-order adapter routing
coverage. `cargo test close_position --lib` passed after tightening
OKX/Bitget/Gate/Bybit/MEXC/HTX close-position adapter ACK assertions for
accepted status, exchange, client order id, exchange order id, command status,
and venue message normalization. `cargo test adapter_should_route_amend_order
--lib`, `cargo test adapter_should_route_set_leverage --lib`, and `cargo test
adapter_position_mode_should_route_and_update_local_state --lib` passed after
standardizing amend/leverage/position-mode ACK assertions for accepted status,
exchange, ids or symbol, leverage/mode, command status where present, and
venue message normalization across Binance and the tracked non-Binance
perpetual adapters. `cargo test native_cancel_all_adapters_should_route_requests
--lib` and `cargo test adapter_should_route_countdown --lib` passed after
standardizing native cancel-all and countdown cancel-all ACK assertions for
exchange, exchange symbol, cancelled count or timeout, trigger time, and venue
message normalization; Binance now has adapter-level countdown cancel-all
routing coverage in addition to request-spec coverage. `cargo test
binance_adapter_should_place_batch_orders_and_split_failures --lib` and
`cargo test binance_adapter_should_batch_cancel_by_order_ids --lib` passed
after tightening Binance batch-place and batch-cancel ACK assertions for main
ack exchange/accepted/counts plus per-order ids, status, accepted flags, and
failure codes/messages. `cargo test
gate_adapter_should_place_batch_orders_and_split_failures --lib` and `cargo
test gate_adapter_should_batch_cancel_by_ids_and_count_failures --lib` passed
after applying the same batch ACK assertion standard to Gate contract-size
batch placement and ID-list batch cancellation. `cargo test
okx_adapter_should_place_batch_orders_and_split_results --lib` and `cargo test
okx_adapter_should_batch_cancel_and_split_item_results --lib` passed after
applying the same batch ACK assertion standard to OKX `sCode`/`sMsg`
batch-order and batch-cancel responses. `cargo test
bitget_should_build_and_parse_batch_place_specs --lib` and `cargo test
bitget_adapter_should_batch_cancel_and_split_item_results --lib` passed after
applying the same batch ACK assertion standard to Bitget `successList` /
`failureList` batch-order and batch-cancel responses. `cargo test
bybit_adapter_should_place_batch_orders_and_split_ret_ext_info --lib` and
`cargo test bybit_adapter_should_batch_cancel_and_split_ret_ext_info --lib`
passed after applying the same batch ACK assertion standard to Bybit
`retExtInfo.list` batch-place and batch-cancel responses. `cargo test
mexc_adapter_should_place_batch_orders_and_split_errors --lib`, `cargo test
mexc_adapter_should_batch_cancel_by_order_ids_and_split_errors --lib`, `cargo
test htx_adapter_should_place_batch_orders_and_split_indexed_results --lib`,
and `cargo test htx_adapter_should_batch_cancel_and_split_indexed_results
--lib` passed after applying the same batch ACK assertion standard to MEXC
`errorCode`/`errorMsg` batch responses and HTX indexed `success`/`errors`
batch responses. `cargo test
bybit_should_parse_private_events --lib` passed after
expanding Bybit private WS parser fixtures from order-only to
order/fill/position/balance coverage. `cargo test
bitget_should_parse_order_position_and_balance_events --lib` passed after
expanding Bitget private WS parser fixtures from order-only to
order/fill/position/balance/control coverage, including login and subscribe
ack normalization. `cargo test gate_should_parse_fill_event
--lib` passed after expanding Gate private WS parser fixtures from fill-only to
order/fill/position/balance/control coverage. `cargo test
mexc_should_parse_private_events --lib` passed after expanding MEXC private WS
parser fixtures from order-only to order/fill/position/balance/control
coverage, including successful subscribe ack filtering. `cargo test
htx_should_parse_private_events --lib` passed after
expanding HTX private WS parser fixtures from order-only to
order/fill/position/balance/control coverage, including auth ack and
top-level ping/pong normalization. `cargo test private_perp --lib`
reports 198 passed tests. `cargo test
private_ws_should_parse_heartbeat_and_subscribe_errors --lib` passed after
adding Bybit private WS pong, subscribe-ack, and subscribe-error control
coverage plus Gate contract-size wrapper subscribe success/error control
coverage. `cargo test htx_symbol_scoped_specs_should_validate_exchange_scope
--lib` passed after adding HTX global cancel-all command exchange-scope
validation. `cargo test gate_should_build_batch_place_spec --lib`, `cargo
test mexc_symbol_scoped_specs_should_validate_exchange_scope_and_batch_ids
--lib`, and `cargo test htx_symbol_scoped_specs_should_validate_exchange_scope
--lib` passed after adding Gate/MEXC/HTX batch-place command exchange, item
symbol, empty-batch, and venue batch-size boundary coverage.
Gate/MEXC/HTX request-spec scope now also covers cancel-batch command exchange
validation plus Gate query-order, fills, and countdown-cancel query/command
exchange validation; targeted `cargo test
gate_symbol_scoped_specs_should_validate_exchange_scope --lib`, `cargo test
mexc_symbol_scoped_specs_should_validate_exchange_scope_and_batch_ids --lib`,
`cargo test htx_cancel_should_validate_required_identifiers_and_symbol_scope
--lib`, and `cargo test gate_should_build_amend_and_countdown_cancel_specs
--lib` passed. OKX request-spec scope now also covers cancel-batch command
exchange validation and amend validation ordering so identifier/change checks
run before the unsupported client-id replacement response; targeted `cargo test
okx_cancel_should_validate_required_identifiers_and_exchange_scope --lib`,
`cargo test okx_should_build_full_private_rest_specs --lib`, and `cargo test
private_perp --lib` passed, with the private-perp filter reporting 198 passed
tests. Bitget history/fill request specs now reject reversed time windows before
REST construction, and the non-Binance open-order aggregate route test now
asserts client order id, side, position side, order type, status, quantity,
filled quantity, and price for OKX/Bitget/Gate/Bybit/MEXC/HTX; targeted `cargo test
bitget_should_build_cancel_all_and_fills_query_specs --lib` and `cargo test
non_binance_adapters_should_route_open_order_readbacks --lib` passed. Bybit,
MEXC, and HTX private WS endpoint tests now parse login and subscribe JSON and
assert auth field shape plus channel/topic lists. The centralized
`private_ws_endpoint_matrix_should_cover_verified_common_surface` test now
constructs offline private WS endpoints for Binance, OKX, Bitget, Gate, Bybit,
MEXC, and HTX, asserting each venue's login requirement and private channel
subscription count; targeted `cargo test
new_private_ws_endpoints_should_login_and_subscribe --lib` and `cargo test
private_ws_endpoint_matrix_should_cover_verified_common_surface --lib` passed.

## Parallel Follow-Up Queue

1. Perpetual parity pass: the offline Binance USD-M common 90% private-perp
   baseline has now been cross-audited against OKX/Bitget/Gate/Bybit/MEXC/HTX.
   CoinEx and KuCoin are not part of this private-perp set yet; both are
   currently Spot-only adapters and remain explicit USDT perpetual scope gaps.
   The capability matrix is covered by
   `private_perp_capability_matrix_should_match_verified_common_surface`:
   market/limit/post-only/IOC/FOK/reduce-only/hedge/client-id/leverage/close/batch-place
   are common across the tracked venues, while the only intentional capability
   exceptions are Gate position-mode change and MEXC/HTX countdown cancel-all.
   Binance, OKX, Bitget, Gate, Bybit, MEXC, and HTX symbol-scoped request
   specs now reject wrong-exchange symbols across order/batch/history/readback/account/amend
   paths, shared cancel rejects wrong scopes before request construction and
   non-Binance single-cancel adapter routes are covered for valid commands,
   non-Binance single-order and open-order readback adapter routes normalize
   common REST envelopes into shared `OrderState`,
   non-Binance all-orders history adapter routes normalize completed order
   envelopes and assert unified order ids, side/position side, order type,
   status, quantity, filled quantity, price, and average fill price across the
   tracked venues,
   non-Binance fills/positions/balances/trade-fee/symbol-account-config
   adapter readbacks route and assert the common account surface fields across
   all tracked non-Binance venues, non-Binance close-position adapter ACKs
   assert accepted status, ids, command status, and normalized venue messages,
   shared amend/leverage/position-mode adapter ACKs assert accepted status,
   exchange, ids or symbol, leverage/mode, command status where present, and
   normalized venue messages,
   shared cancel-all/countdown adapter ACKs assert exchange, exchange symbol,
   cancelled count or timeout, trigger time, and normalized venue messages,
   Binance, OKX, Bitget, Gate, Bybit, MEXC, and HTX batch-place/batch-cancel adapter ACKs
   assert main counts plus per-order ids, status, accepted flags, and failure
   codes/messages,
   shared query-order/open-orders/order-history/order-amendment-history/positions/fills/trade-fee/symbol-account-config
   readbacks reject wrong scopes before request construction, non-Binance
   order-amendment-history fallbacks reject valid unsupported requests before REST,
   shared cancel-all rejects wrong command/symbol scopes before composed readback,
   shared batch-place and batch-cancel reject wrong command/item exchanges
   before capability checks, and valid empty batch-place/batch-cancel commands
   return empty acks before REST, OKX composed cancel-all returns a zero-cancel
   ack after an empty pending-order readback without batch-cancel, shared order and amend construction reject
   wrong scopes before quantity/capability checks, shared order/batch/amend
   construction validates positive finite quantity and price when present,
   shared leverage changes reject wrong scopes before capability/value checks, shared
   countdown cancel-all rejects wrong scopes before capability/request construction,
   position-mode commands reject wrong exchanges before capability/request construction,
   and shared close-position request construction rejects wrong scopes before
   capability/value checks. Remaining private-perp work is live
   readonly/order-canary validation depth, production reconciliation behavior,
   and venue-specific optional endpoints outside the common Binance baseline.
2. Spot advanced order model: `cancel_all_orders()` is now wired across the
   tracked Spot venues using verified native or composed endpoints, and
   Binance Spot now has shared `QuoteMarketOrderRequest`,
   `AmendOrderRequest`, and `OrderListRequest` paths for quote-sized market
   orders, keep-priority quantity reductions, and OCO/OTO order lists. The
   offline Spot capability matrix is covered by
   `spot_capability_matrix_should_match_binance_parity_baseline`: all tracked
   Spot venues expose the common market/limit/post-only/IOC/FOK/cancel/cancel-all/query/open-orders/balances/public-WS/fee surface;
   Binance alone advertises verified order-list support, and MEXC alone keeps
   amend disabled because Spot V3 has no verified native amend/cancel-replace
   endpoint. Runtime private user-stream capability is enabled only for
   Binance, OKX, and KuCoin; Bitget, Gate.io, MEXC, and CoinEx keep REST
   polling fallbacks while retaining parser fixtures for future enablement.
   OKX, Bitget, Gate.io, MEXC, CoinEx, and KuCoin now map the quote-sized
   market-buy subset where native semantics are verified. Non-Binance Spot
   fallbacks validate the shared quote-market/amend/order-list/cancel-all and
   orderbook subscription/recent-fill requests before returning explicit
   unsupported, including client-order-id and shared order-list leg
   price/stop-price requirements. The live readonly harness now blocks
   `place_order`, `place_quote_market_order`, `amend_order`, `place_order_list`,
   `cancel_order`, and `cancel_all_orders` with classified `PermissionDenied`
   `readonly_guard` errors before any wrapped client mutation can run, and the
   generated report records `mutation_calls_detected` from the same guard
   detector. Remaining Spot work is live validation depth plus optional advanced
   order-list models only on venues with verified native equivalents.
3. OKX Perpetual: now has a side-effect-free `private_perp` protocol and
   gateway registration. Next OKX work is live readonly/order-canary validation
   plus production reconciliation around composed cancel-all and private stream
   ordering.
4. Bitget/Gate/MEXC/CoinEx Spot: order-type parity is covered offline for
   post-only/IOC/FOK semantics; keep them as Binance Spot parity baselines
   except private user streams, which are currently documented REST-polling
   fallbacks.
5. KuCoin Spot: run live private-stream validation and keep REST
   polling/reconciliation fallback enabled until order/fill/balance events are
   observed with production credentials.
6. MEXC/HTX Perpetual: MEXC client-id batch cancel is covered by a composed
   `externalOid` fallback that preflights every item before REST, MEXC native
   batch-cancel now requires exchange order ids for every item, and MEXC amend now requires a changed price or
   quantity. MEXC/HTX countdown capability gates now reject valid countdown
   requests at the adapter layer before REST. HTX cancel, single-order readback, and
   symbol-scoped account/amend/countdown paths validate identifier and symbol
   scope. MEXC countdown and HTX amend/countdown are covered by explicit
   unsupported tests, including HTX adapter-layer REST prevention, after checking for
   verified venue equivalents. Fill only if venue docs expose compatible native endpoints
   later.
7. BitMart/Hyperliquid: keep them outside the current Binance-parity
   percentages until their legacy `Exchange` implementations are migrated or
   wrapped behind the unified Spot/perpetual contracts with request-spec tests.
