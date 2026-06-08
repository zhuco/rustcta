# XT.com Gateway Adapter

Status: industrial `ExchangeClient` adapter for XT.com Spot and USDT-M
perpetual REST plus WebSocket subscription specs/parsers.

## Scope

The adapter id is `xt`. It is implemented under
`crates/rustcta-exchange-gateway/src/adapters/xt/` and registered through
`AdapterBackedGateway::register_named_adapter("xt")`.

Current capabilities:

- Spot and USDT-M perpetual symbol rules.
- Spot and USDT-M perpetual order book snapshots.
- Spot and USDT-M balances, perpetual positions, fee metadata, order lifecycle,
  quote-sized Spot market buy, amend order, native batch place/cancel,
  open orders, query order, cancel all, and recent fills when private REST
  credentials are configured.
- Separate XT Spot and Futures HMAC-SHA256 signing formats.
- Spot and USDT-M public/private WebSocket subscription payloads, private
  listenKey acquisition, ping/pong heartbeat policy, and stream parsers for
  public book/trade plus private order/fill/balance/position events.
- Request-spec/parser tests for public REST, private REST, WebSocket, and error
  classification.
- Machine-readable endpoint mapping at
  `crates/rustcta-exchange-gateway/src/adapters/xt/endpoint_mapping.yaml` and
  parser fixtures under `tests/fixtures/exchanges/xt/`.

Binance-style OCO/OTO order lists, leverage/margin/position-mode mutations,
perpetual quote-sized market orders, and reduce-only order submission are not
enabled. XT Futures has native TP/SL and trigger-order APIs, but they do not
match the current Binance-style `OrderListRequest` contract, so
`place_order_list` returns explicit `Unsupported` instead of silently
degrading.

Conservative unsupported boundaries:

- Non-Spot/non-Perpetual markets return `Unsupported`.
- Spot positions and Spot position streams return `Unsupported`.
- Perpetual quote-sized market orders return `Unsupported`.
- Order-list/OCO/OTO, leverage, margin-mode, position-mode, transfer, and
  withdrawal operations are not declared.
- Reduce-only submission is not enabled because the shared capability must be
  truthful across both supported products.

## Endpoint Mapping

| Standard capability | XT Spot endpoint | XT USDT-M endpoint |
| --- | --- | --- |
| `get_symbol_rules` | `GET /v4/public/symbol` | `GET /future/market/v3/public/symbol/list` |
| `get_order_book` | `GET /v4/public/depth` | `GET /future/market/v1/public/q/depth` |
| `get_balances` | `GET /v4/balances` | `GET /future/user/v1/balance/list` |
| `get_positions` | `Unsupported` | `GET /future/user/v1/position/list` |
| `get_fees` | public symbol fee metadata | public symbol fee metadata |
| `place_order` | `POST /v4/order` | `POST /future/trade/v1/order/create` |
| `place_quote_market_order` | `POST /v4/order` with `quoteQty` for market buy | `Unsupported` |
| `amend_order` | `PUT /v4/order/{orderId}` | `POST /future/trade/v1/order/update` |
| `cancel_order` | `DELETE /v4/order/{orderId}` | `POST /future/trade/v1/order/cancel` |
| `batch_place_orders` | `POST /v4/batch-order` | `POST /future/trade/v2/order/atomic-create-batch` |
| `batch_cancel_orders` | `DELETE /v4/batch-order` | `POST /future/trade/v1/order/cancel-batch` |
| `cancel_all_orders` | `DELETE /v4/open-order` | `POST /future/trade/v1/order/cancel-all` |
| `query_order` | `GET /v4/order/{orderId}` or `GET /v4/order` | `GET /future/trade/v1/order/detail` |
| `get_open_orders` | `GET /v4/open-order` | `POST /future/trade/v1/order/list-open-order` |
| `get_recent_fills` | `GET /v4/trade` | `GET /future/trade/v1/order/trade-list` |
| `place_order_list` | no verified Spot OCO/OTO equivalent | no Binance OCO/OTO equivalent |
| `subscribe_public_stream` | `wss://stream.xt.com/public` topics `depth_update`, `depth`, `trade`, `ticker`, `kline` | `wss://fstream.xt.com/ws/market` topics `depth_update`, `depth`, `trade`, `agg_ticker`, `kline` |
| `subscribe_private_stream` | `POST /v4/ws-token`, then `wss://stream.xt.com/private` topics `order`, `trade`, `balance` | `GET /future/user/v1/user/listen-key`, then `wss://fstream.xt.com/ws/user` topics `order`, `trade`, `balance`, `position`, `notify` |

The YAML mapping splits Spot and USDT-M futures into separate rows, for
example `xt.place_order_spot` and `xt.place_order_futures`, so request-spec,
signing, rate-limit and parser coverage can be audited per product.

Default base URLs:

- Spot: `https://sapi.xt.com`
- USDT-M perpetual: `https://fapi.xt.com`
- Spot public/private WS: `wss://stream.xt.com/public`,
  `wss://stream.xt.com/private`
- USDT-M public/private WS: `wss://fstream.xt.com/ws/market`,
  `wss://fstream.xt.com/ws/user`

## Signing

Spot private REST signs:

```text
validate-algorithms=HmacSHA256&validate-appkey=<key>&validate-recvwindow=<ms>&validate-timestamp=<ms>#METHOD#path#query-or-body
```

Futures private REST signs:

```text
validate-appkey=<key>&validate-timestamp=<ms>#path#query-or-body
```

Both signatures are lowercase hex HMAC-SHA256 with the API secret. The adapter
keeps secrets out of request path, query, and body.

Signing vectors are covered in
`crates/rustcta-exchange-gateway/src/adapters/xt/signing.rs` and request-spec
tests assert Spot signed JSON bodies, Futures form bodies, GET query signing,
and private WS token/listen-key acquisition.

## Batch, Pagination, Rate Limit, Reconciliation

Batch support is native for both products:

- Spot place: `POST /v4/batch-order`; Spot cancel: `DELETE /v4/batch-order`.
- Futures place: `POST /future/trade/v2/order/atomic-create-batch`; Futures
  cancel: `POST /future/trade/v1/order/cancel-batch`.
- The adapter requires all items in one batch to share a market type, supports
  client order ids, declares max items as 20 until XT-specific limits are
  live-verified, and declares partial failure support because Spot returns
  per-item acknowledgements and normalized responses may contain mixed results.
- Batch cancel requires `exchange_order_id` for every item.

Pagination is conservative:

- Order book uses depth/level limits: Spot max 500, Futures max 50.
- Recent fills accepts `startTime`, `endTime`, `orderId`, and `limit`, clamped
  to 1000.
- Open orders and order query do not expose cursor pagination in the adapter.

Rate limits are declared as conservative buckets in endpoint mapping:
`xt_public_rest`, `xt_private_rest`, and `xt_trade`, with default weight 1.
No global shared limiter is extended by this migration.

REST reconciliation fallback:

- Public order-book gaps or reconnects resync through `get_order_book`.
- Private Spot reconnects reconcile through balances, open orders, query order,
  and recent fills.
- Private Futures reconnects reconcile through balances, positions, open
  orders, query order, and recent fills.
- Live-dry-run should keep exchange kill-switch, disabled-symbol and
  max-notional guards enabled before private order submission.

Error classification maps authentication/signature/key failures to
`Authentication`, HTTP 429 or "too many/rate" messages to `RateLimited`,
invalid symbols to `InvalidSymbol`, insufficient balance messages to
`InsufficientBalance`, not-found order messages to `OrderNotFound`, precision
or scale failures to `InvalidPrecision`, malformed request/order/common errors
to `InvalidRequest`, and 5xx statuses to `ExchangeUnavailable`.

## WebSocket

Public WebSocket subscriptions build official XT topic strings and private
subscriptions first acquire the required listen key. Heartbeat payload is the
text frame `ping`; the parser recognizes `pong`. The adapter exposes heartbeat
policy values for the shared gateway stream supervisor: Spot pings before the
1-minute timeout, and Futures pings before the 30-second timeout.

Private stream auth is listen-key/token based. Spot acquires
`POST /v4/ws-token`; Futures acquires
`GET /future/user/v1/user/listen-key`. Renewal is declared at 30-minute
intervals in capability metadata and endpoint mapping; reconnect requires
re-authentication and resubscription. If private credentials are absent,
private REST and private streams are declared `Unsupported`.

P9 official verification adds the public order book rebuild rule: subscribe to
`depth_update@btc_usdt`, buffer events, fetch a REST snapshot with limit 500,
drop stale updates, and enforce continuity. Spot uses `fi/i`; Futures uses
`fu/u`. The project already has 1000ms interval evidence; no faster fixed
official interval or checksum was confirmed in this batch.

## Configuration

`config/xt_gateway_example.yml` provides a disabled-private-REST example.
Credentials are read from:

```bash
export XT_API_KEY=...
export XT_API_SECRET=...
```

Use read/trade scoped keys only, keep withdrawal permission disabled, and run
read-only/live-dry-run validation before real order submission.

## Validation

Targeted local validation:

```bash
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/xt-task-check cargo test -p rustcta-exchange-gateway adapters::xt:: --lib --message-format short
python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/xt/endpoint_mapping.yaml
```

The ignored `xt_live_readonly_should_cover_public_and_authenticated_baseline`
test can be run manually with `XT_API_KEY` and `XT_API_SECRET` for read-only
preflight.
Latest targeted run passed 18 XT adapter tests with one ignored live-readonly
preflight, 733 filtered out, and existing workspace warnings.
