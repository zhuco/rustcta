# BingX Gateway Adapter

Status date: 2026-06-07

Adapter id: `bingx`

Implementation status: Spot + USDT linear perpetual REST and WebSocket
request-spec/parser support are implemented behind
`rustcta-exchange-api::ExchangeClient`. The adapter covers public stream
subscription specs, private listen-key subscription specs, heartbeat response,
and focused parser fixtures; a long-running reconnect supervisor remains a
gateway platform concern.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST + private REST + public/private stream specs |
| USDT linear perpetual | `Perpetual` | Public REST + private REST + public/private stream specs |
| Testnet | n/a | Unsupported; no separate official REST host verified |

REST base URL: `https://open-api.bingx.com`

Fallback domain noted by BingX: `open-api.bingx.io`, not enabled by default.

## Authentication

Private requests use:

- Header: `X-BX-APIKEY`
- Timestamp: `timestamp` in milliseconds
- Optional validity window: `recvWindow`
- Signature: lowercase hex `HMAC-SHA256(secret, sorted_query_without_signature)`

The adapter sorts all query parameters by key, appends `timestamp` and
`recvWindow`, signs the raw query string, then appends `signature`.

## Endpoint Mapping

Structured mapping lives in
`crates/rustcta-exchange-gateway/src/adapters/bingx/endpoint_mapping.yaml`.
The adapter-scoped capability declaration lives in
`crates/rustcta-exchange-gateway/src/adapters/bingx/capabilities_v2.yaml`.

| Standard capability | Spot endpoint | Perpetual endpoint |
| --- | --- | --- |
| server time | Uses swap time fallback | `GET /openApi/swap/v2/server/time` |
| symbol rules | `GET /openApi/spot/v1/common/symbols` | `GET /openApi/swap/v2/quote/contracts` |
| order book | `GET /openApi/spot/v1/market/depth` | `GET /openApi/swap/v2/quote/depth` |
| balances | `GET /openApi/spot/v1/account/balance` | `GET /openApi/swap/v2/user/balance` |
| positions | Unsupported | `GET /openApi/swap/v2/user/positions` |
| fees | `GET /openApi/spot/v1/user/commissionRate` | `GET /openApi/swap/v2/user/commissionRate` |
| place order | `POST /openApi/spot/v1/trade/order` | `POST /openApi/swap/v2/trade/order` |
| cancel order | `POST /openApi/spot/v1/trade/cancel` | `DELETE /openApi/swap/v2/trade/order` |
| cancel all | `POST /openApi/spot/v1/trade/cancelOpenOrders` | `DELETE /openApi/swap/v2/trade/allOpenOrders` |
| query order | `GET /openApi/spot/v1/trade/query` | `GET /openApi/swap/v2/trade/order` |
| open orders | `GET /openApi/spot/v1/trade/openOrders` | `GET /openApi/swap/v2/trade/openOrders` |
| fills | `GET /openApi/spot/v1/trade/myTrades` | `GET /openApi/swap/v2/trade/allFillOrders` |
| batch place | `POST /openApi/spot/v1/trade/batchOrders` | `POST /openApi/swap/v2/trade/batchOrders` |
| batch cancel | `POST /openApi/spot/v1/trade/cancelOrders` | `DELETE /openApi/swap/v2/trade/batchOrders` |
| amend | Unsupported | `POST /openApi/swap/v1/trade/amend` |
| OCO order list | `POST /openApi/spot/v1/oco/order` | Unsupported |
| leverage | Unsupported | `POST /openApi/swap/v2/trade/leverage` |
| position mode | Unsupported | `POST/GET /openApi/swap/v1/positionSide/dual` |
| multi-assets mode | Unsupported | `POST/GET /openApi/swap/v1/trade/assetMode` |
| margin type | Unsupported | `POST /openApi/swap/v2/trade/marginType` |

Advanced native countdown cancel-all and OTO order lists are not advertised in
`ExchangeClientCapabilities`; unsupported market/product combinations return
`Unsupported`.

## Capabilities V2

The local `capabilities_v2.yaml` declares Spot and USDT linear perpetual
support, public REST symbol/order-book reads, credential-gated private REST,
public/private stream runtime capability, rate-limit, pagination,
reconciliation, and batch plans. Unsupported boundaries are testnet host,
options, delivery futures, spot positions, spot amend, and perpetual OCO.

## WebSocket

Public subscription specs:

- Spot: `BTC-USDT@trade`, `@ticker`, `@depth`, `@depth5@500ms`,
  `@kline_1m`.
- Perpetual: `BTC-USDT@trade`, `@ticker`, `@depth50`, `@depth5@500ms`,
  `@kline_1m`.

Private subscription specs use `POST /openApi/user/auth/userDataStream` to
create a listen key, then subscribe to Spot `spot.executionReport` or perpetual
`ORDER_TRADE_UPDATE`/`ACCOUNT_UPDATE`. Text heartbeat `Ping`/`ping` maps to
`Pong`; JSON ping/pong and subscription acks are parsed as control messages.

Runtime policy:

- Public WS supports spot/perpetual trades, ticker, candles, order book delta,
  and order book snapshot subscription payloads.
- Public order book runtime refreshes REST snapshot on connect, sequence gap,
  reconnect, or checksum/runtime validation failure.
- Private WS is credential-gated and uses listen-key auth. Renewal recreates the
  listen key before expiry; if renewal fails, runtime falls back to REST
  reconciliation.
- Reconnect policy is resubscribe all channels, then refresh snapshots and
  private order/account state through REST.

## Rate Limit, Pagination, Reconciliation, Batch

The adapter declares conservative local plans in `endpoint_mapping.yaml`:

- Rate limits: public REST uses `bingx_rest_ip`, signed REST uses
  `bingx_rest_uid`, and WS control messages use `bingx_ws_connection`.
- Pagination: order book uses `limit`; recent fills use time windows
  (`startTime`/`endTime` for Spot, `startTs`/`endTs` for perpetual) capped at
  `limit=1000`; other current REST reads are non-paginated.
- Reconciliation: private WS is primary; fallback sources are `query_order`,
  `get_open_orders`, and `get_recent_fills`. Reconcile after ambiguous
  place/cancel/batch acknowledgements, WS gaps/reconnects, and listen-key
  renewal failures.
- Batch: Spot and perpetual batch place/cancel are native, partial-atomic, and
  capped locally at 20 items until live validation proves a larger bound.

## Error Classification

The transport maps:

- HTTP `401` or code `100001` to authentication failure.
- HTTP `403` to permission failure.
- HTTP `418`/`429` to rate limit.
- HTTP `5xx`, `100500`, and `80012` to exchange unavailable.
- `80014` to invalid request.
- `80016` to order not found.
- `80017` to insufficient position.
- Message heuristics cover insufficient balance, duplicate client order id,
  invalid symbol, invalid precision, and rate-limit wording.

## Validation

Offline request-spec tests cover public Spot rules, public perpetual depth,
signed private readbacks, signed order mutations, Spot/perp batch place/cancel,
perp amend, Spot OCO, leverage/margin/position-mode helpers, stream
subscription payloads, heartbeat handling, unsupported private behavior without
credentials, fixed signing vectors, parser fixtures for success/empty/error
responses/critical missing fields, and static endpoint/capability plan
declarations.

Validation commands:

- `cargo fmt --check`
- `cargo test -p rustcta-exchange-gateway bingx_ --lib`
- `cargo check -p rustcta-exchange-gateway`
- Optional authenticated live gate:
  `cargo test -p rustcta-exchange-gateway bingx_live_readonly --lib -- --ignored`

Public read-only live validation report:
`logs/bingx_readonly_public_2026-06-07.json`.

Authenticated read-only validation is not complete in this workspace because
`BINGX_API_KEY` and `BINGX_API_SECRET` are not present. The adapter keeps private
operations unsupported without credentials and the live report records
`missing_credentials_private_readonly_skipped`.
