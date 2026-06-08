# WOO X Gateway Adapter

`crates/rustcta-exchange-gateway/src/adapters/woo/` implements the industrial
gateway `ExchangeClient` adapter for WOO X V3 Spot and USDT perpetual REST plus
public/private WebSocket subscription, heartbeat, and parser/session helpers.

## Scope

- REST public: instruments and order book snapshots for `SPOT_*` and `PERP_*`
  symbols.
- REST private: balances, fee rate, place order, quote-sized Spot market order,
  composed batch place/cancel, cancel order, symbol-scoped cancel-all, amend
  quantity, query order, open orders, recent fills, perpetual positions, and
  adapter-specific V3 algo stop-order helpers.
- Advanced private REST helpers: cancel-all-after, futures leverage, account
  position mode, account trading mode, and default margin mode readback.
- REST signing: `x-api-key`, `x-api-timestamp`, and `x-api-signature` using
  hex HMAC-SHA256 over `timestamp + METHOD + path_with_query + body`.
- Streams: public `trade/ticker/orderbook10/orderbookupdate/kline`, private
  `executionreport/balance/position/account`, listen-key private URL, PING
  heartbeat payload, PONG/heartbeat parsing, and reusable session helpers that
  emit standard `ExchangeStreamEvent` values for order book, order, fill,
  balance, position, and heartbeat updates.

## Endpoint Mapping

| Standard capability | WOO X V3 endpoint |
| --- | --- |
| `get_symbol_rules` | `GET /v3/public/instruments` |
| `get_order_book` | `GET /v3/public/orderbook` |
| `get_balances` | `GET /v3/asset/balances` |
| `get_positions` | `GET /v3/futures/positions` |
| `get_fees` | `GET /v3/trade/tradingFee` |
| `place_order` | `POST /v3/trade/order` |
| `place_quote_market_order` | `POST /v3/trade/order` with `amount` |
| `batch_place_orders` | Composed sequential `POST /v3/trade/order`; non-atomic |
| `cancel_order` | `DELETE /v3/trade/order` |
| `batch_cancel_orders` | Composed sequential `DELETE /v3/trade/order`; non-atomic |
| `cancel_all_orders` | `DELETE /v3/trade/allOrders` |
| `amend_order` | `PUT /v3/trade/order` |
| `query_order` | `GET /v3/trade/order` |
| `get_open_orders` | `GET /v3/trade/orders` with `status=INCOMPLETE` |
| `get_recent_fills` | `GET /v3/trade/transactionHistory` |
| `set_cancel_all_after` | `POST /v3/trade/cancelAllAfter` |
| `set_futures_leverage` | `PUT /v3/futures/leverage` |
| `set_position_mode` | `PUT /v3/futures/positionMode` |
| `set_account_trading_mode` | `POST /v3/account/tradingMode` |
| `get_default_margin_mode` | `GET /v3/futures/defaultMarginMode` |
| `place_algo_order` | `POST /v3/trade/algoOrder` |
| `amend_algo_order` | `PUT /v3/trade/algoOrder` |
| `query_algo_order` | `GET /v3/trade/algoOrder` |
| `cancel_algo_order` | `DELETE /v3/trade/algoOrder` |
| `get_algo_orders` | `GET /v3/trade/algoOrders` |
| `cancel_algo_orders` | `DELETE /v3/trade/algoOrders` |
| public streams | `wss://wss.woox.io/v3/public`, `SUBSCRIBE` topics |
| private streams | `POST /v3/account/listenKey`, `wss://wss.woox.io/v3/private?key=...` |

## Symbol Projection

WOO X native symbols include the product prefix:

- Spot: `SPOT_BTC_USDT`
- Perpetual: `PERP_BTC_USDT`

The adapter can infer prefixed WOO symbols from canonical `BTC/USDT` scopes
when the market type is `Spot` or `Perpetual`. Loaded symbol rules normalize
base/quote assets, price tick, quantity step, min quantity, max quantity, and
min notional.

## Credentials

Private REST requires:

```bash
export WOO_API_KEY=...
export WOO_API_SECRET=...
```

`WOOX_API_KEY`, `WOOX_API_SECRET`, `WOO_SPOT_API_KEY`, and
`WOO_SPOT_API_SECRET` are accepted as fallbacks. Use read/trade-only keys for
live dry-run; withdrawal permission must be disabled.

## Capability Notes

- `capabilities_v2` declares read-only/trade credential scopes, signed endpoint
  metadata, cursor/limit fills history, composed batch semantics, and private
  stream runtime details.
- Spot and perpetual share the V3 trading endpoints; the adapter routes by
  `MarketType` and native symbol prefix.
- `batch_place_orders` and `batch_cancel_orders` are composed through verified
  single-order V3 endpoints because the public WOO V3 ordinary order API does
  not expose a Binance-equivalent atomic batch create/cancel by explicit
  order-id list. The calls are best-effort and non-atomic.
- `clientOrderId` is forwarded as a string. WOO's V3 schema documents numeric
  client IDs in places, so live validation should confirm account-specific
  behavior before enabling real orders.
- Public `orderbookupdate` is parsed into the standard stream shape, but it is
  not declared as a strict Binance diff-depth equivalent because live consumers
  still need snapshot buffering and continuity checks before applying deltas.
- Private WebSocket requires trade/account credentials because the private URL
  is derived from a signed listen-key request.
- Algo orders are exposed as WOO-specific helper methods because the current
  `ExchangeClient` trait has no common trigger-order model. They use the
  current V3 `/v3/trade/algoOrder(s)` endpoints and remain outside the shared
  order-list capability flags.

## Toolchain Metadata

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/woo/endpoint_mapping.yaml`
  records REST/WS operation coverage, auth class, rate-limit bucket, native
  batch/atomicity, request-spec status, parser fixture, pagination, and
  reconciliation metadata.
- Rate limits are modeled as separate public, private-read, and trade buckets;
  batch place/cancel are explicitly non-atomic composed loops with max 20
  items.
- Private streams use a signed `listenKey`; metadata declares client PING,
  listen-key renewal through `/v3/account/listenKey`, reconnect login, and
  resubscribe requirements.
- Reconciliation after reconnect uses REST snapshot for order books and REST
  open-order/fill readback for private state.
- Live-dry-run promotion requires the shared runner controls: reconciliation
  enabled, kill-switch active, disabled-symbol filtering, and max-notional
  limits.

## Validation

Targeted local validation:

```bash
python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/woo/endpoint_mapping.yaml
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/woo-task-check cargo test -p rustcta-exchange-gateway woo --lib --message-format short
cargo check -p rustcta-exchange-gateway
```

The WOO tests cover signed request construction, secret-free request paths and
bodies, Spot and perpetual symbol parsing, order book parsing, private readback
parsing, order mutations, composed batch order/cancel, quote-sized market
orders, perpetual positions, advanced private control endpoints, V3 algo order
helpers, public/private stream subscription payloads, heartbeat, listen-key
private sessions, and standard stream-event conversion.
Latest targeted run passed 18 WOO tests with 734 filtered out and existing
workspace warnings.
