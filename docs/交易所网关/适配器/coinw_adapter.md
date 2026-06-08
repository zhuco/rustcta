# CoinW Gateway Adapter

Status: Spot + USDT perpetual REST adapter plus WebSocket session,
heartbeat, and parser specs for the unified gateway.

## Scope

The adapter id is `coinw`. It is implemented under
`crates/rustcta-exchange-gateway/src/adapters/coinw/` and registered through
`AdapterBackedGateway::register_named_adapter("coinw")`.

Current capabilities:

- Spot symbol rules via `GET /api/v1/public?command=returnSymbol`
- Spot order book snapshot via `GET /api/v1/public?command=returnOrderBook`
- Perpetual symbol rules via `GET /v1/perpum/instruments`
- Perpetual order book snapshot via `GET /v1/perpumPublic/depth`
- Spot balances, order lifecycle, cancel-all, open/query orders, quote-sized
  market orders, and recent fills via signed `/api/v1/private?command=...`
- USDT perpetual balances, positions, fee readback, order lifecycle, open/query
  orders, batch cancel, cancel-all composition, and recent fills via signed
  `/v1/perpum/...`
- Gateway-level batch place fallback where CoinW has no uniform native batch
  endpoint; Spot batch cancel fallback; Perp batch cancel sends CoinW native
  `/v1/perpum/batchOrders` across `plan`, `execute`, `PostOnly`, `IOC`, and
  `FOK` position types so ordinary and advanced Perp orders are covered.
- Spot and Futures public/private WebSocket subscription request specs,
  session `initial_requests`, ping/pong heartbeat helpers, reconnect policy,
  public order book parser, and private order/balance/position conversion to
  standard `ExchangeStreamEvent`.
- CoinW Spot private signing helper: sorted params + `secret_key`, MD5 uppercase
- CoinW Futures private signing helper: base64 HMAC-SHA256 over
  `timestamp + METHOD + path_with_query/body`
- Adapter-local toolchain metadata:
  `crates/rustcta-exchange-gateway/src/adapters/coinw/endpoint_mapping.yaml`
  and `capabilities_v2` declarations for REST, WS, batch, history, rate-limit,
  pagination, and reconciliation coverage.

The independent WebSocket runtime loop, amend order, OCO/OTO order lists,
reduce-only order placement, and leverage/margin/position-mode mutations are
intentionally not enabled in this first pass. Spot advanced TIF/post-only
orders are also rejected explicitly because CoinW Spot does not expose the same
semantics as Perp. These return `Unsupported` through the standard
`ExchangeClient` surface or remain platform follow-ups.

## Endpoint Mapping

| Standard capability | CoinW endpoint | Notes |
| --- | --- | --- |
| `get_symbol_rules` Spot | `GET /api/v1/public?command=returnSymbol` | Parses `currencyPair`, base/quote, price/count precision, min/max quantity, min/max notional, and active state. |
| `get_order_book` Spot | `GET /api/v1/public?command=returnOrderBook` | Sends `symbol` and official `size` of `5` or `20`; response has no exchange timestamp. |
| `get_symbol_rules` Perp | `GET /v1/perpum/instruments` | Parses USDT perpetual metadata as `MarketType::Perpetual`; filters non-`online` instruments. |
| `get_order_book` Perp | `GET /v1/perpumPublic/depth` | Sends `base=BTC` for USDT contracts and parses `p`/`m` levels plus `ts`. |
| `get_balances` Spot | `POST /api/v1/private?command=returnCompleteBalances` | Parses `available` and `onOrders` into available/locked balances. |
| `get_balances` Perp | `GET /v1/perpum/account/getUserAssets` | Parses USDT account equity, available, and frozen margin fields. |
| `get_positions` Perp | `GET /v1/perpum/positions`, `/positions/all` | Parses current USDT perpetual positions. |
| `get_fees` Spot/Perp | Spot fee default, Perp `GET /v1/perpum/account/fees` | Spot docs do not expose a private fee endpoint; source is marked default. |
| `place_order` Spot | `POST /api/v1/private?command=doTrade` | Limit/market orders; requires `client_order_id` as CoinW `out_trade_no`. |
| `place_quote_market_order` Spot | `POST /api/v1/private?command=doTrade` | Uses market `funds` for quote-sized buy/sell orders. |
| `place_order` Perp | `POST /v1/perpum/order` | Maps market/limit/IOC/FOK/PostOnly to CoinW `positionType`; default leverage is `1` in the standard request mapping. |
| `cancel_order` Spot/Perp | Spot `cancelOrder`; Perp `DELETE /v1/perpum/order` | Requires exchange order id. |
| `cancel_all_orders` Spot/Perp | Spot `cancelAllOrder`; Perp composed open-orders + `DELETE /v1/perpum/batchOrders` | Perp has no single cancel-all endpoint in the docs. |
| `query_order` Spot/Perp | Spot `returnOrderStatus`; Perp `GET /v1/perpum/order` | Requires exchange order id. |
| `get_open_orders` Spot/Perp | Spot `returnOpenOrders`; Perp `GET /v1/perpum/orders/open` | Symbol-scoped. |
| `get_recent_fills` Spot/Perp | Spot `returnUTradeHistory`; Perp `GET /v1/perpum/orders/deals` | Symbol-scoped. |
| Batch place/cancel | Gateway sequential fallback; Perp native batch cancel | Batch place is intentionally composed from standard single-order calls; Perp batch cancel sends all supported CoinW position types to avoid missing non-`plan` orders. |
| WebSocket specs | Spot/Futures public/private streams | Subscription ids and payload specs for public book/trades/ticker/candles and private order/assets/position channels; session helpers provide `initial_requests`, ping/pong heartbeat, reconnect policy, public order book parsing, and private order/balance/position standard events. |

Default REST base URL: `https://api.coinw.com`.

## Task 16 Toolchain Declarations

- Dual product endpoint mapping is declared in
  `crates/rustcta-exchange-gateway/src/adapters/coinw/endpoint_mapping.yaml`
  with separate Spot and USDT perpetual entries.
- Spot private REST uses query signing: sorted params, `secret_key`, uppercase
  MD5, query `api_key` and `sign`.
- Futures private REST uses header signing: base64 HMAC-SHA256 over
  `timestamp + METHOD + path_with_query + body`, sent as `api_key`,
  `timestamp`, and `sign`.
- Conservative `Unsupported` is retained for amend order, order-list/OCO/OTO,
  perpetual quote-sized market order, unsupported market types, and Spot
  private positions stream.
- Batch place is composed sequentially from single order calls, non-atomic,
  max 20, with partial failure requiring reconciliation.
- Batch cancel is composed sequentially. Spot uses single cancel fallback;
  perpetual uses native `/v1/perpum/batchOrders` in chunks of 20 for `plan`,
  `execute`, `PostOnly`, `IOC`, and `FOK`, with partial failure requiring
  reconciliation.
- Error classification maps HTTP 401/403 and signature/API-key messages to
  authentication, permission text to permission, insufficient balance text to
  insufficient balance, missing order text to order-not-found, symbol/pair/
  instrument text to invalid symbol, precision text to invalid precision, and
  HTTP 418/429, code `29001`, or frequency/rate messages to rate limited.
- Rate limits are declared conservatively as fixed weight 1 buckets:
  `coinw_spot_public`, `coinw_spot_private`, `coinw_futures_public`, and
  `coinw_futures_private`.
- Pagination is limited to recent fills: Spot `limit`, futures `pageSize`,
  max 100. Order/open-order reconciliation is symbol/order scoped with no
  cursor support declared.
- REST reconciliation is the source of truth: balances, positions, query
  order, open orders, and recent fills must be used after WS reconnect or
  before live-dry-run.
- Public WS capability is declared for Spot and perpetual subscriptions with
  ping/pong heartbeat, reconnect, resubscribe, and REST order book resync.
- Private WS capability is declared as `rest_fallback`: private parser/session
  specs exist, authentication requires credentials, no listen-key renewal is
  used, and reconnect requires re-login plus REST reconciliation.
- Live-dry-run remains gated on reconciliation, kill-switch, disabled-symbol,
  and max-notional controls.

## Fixtures

Fixture coverage lives in `tests/fixtures/exchanges/coinw/`:

- Success: Spot/perp symbol rules, Spot/perp order book, Spot/perp balances,
  and perpetual positions.
- Empty response: `empty_data_array.json`.
- Error response: `error_rate_limited.json`.
- Missing field: `missing_symbol_field.json`.
- Request specs: `request_specs/spot_place_order_limit.json` and
  `request_specs/futures_place_order_limit.json`.
- Signing vectors: `signing_vectors/spot_md5_query.json` and
  `signing_vectors/futures_hmac_headers.json`.

The adapter tests load these fixtures with `include_str!` and cover parser
success, empty payload behavior, missing-field decode errors, error
classification, request/spec signing headers/query parameters, and the
capability/batch declarations.

## Configuration

`config/spot_exchanges_example.yml` includes a disabled-by-default `coinw`
example. The gateway config reads optional `COINW_API_KEY`,
`COINW_API_SECRET`, and `COINW_PRIVATE_REST_ENABLED`; private capabilities are
advertised only when credentials and private REST are enabled.

## Validation

Offline tests cover request construction, parser behavior, adapter registration,
WebSocket subscription/session/heartbeat/parser specs, and signature vectors.
The CoinW-specific validation commands are:

```bash
cargo fmt
cargo test -p rustcta-exchange-gateway coinw_ --lib
cargo check -p rustcta-exchange-gateway --lib
```
