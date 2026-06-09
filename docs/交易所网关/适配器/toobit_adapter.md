# Toobit Gateway Adapter

Status date: 2026-06-07

Adapter id: `toobit`

Implementation status: Spot + USDT-M perpetual REST and WebSocket request-spec
adapter is implemented behind `rustcta_exchange_api::ExchangeClient` in
`rustcta-exchange-gateway`. It covers public symbol rules/order book, private
balances/positions/fees/order lifecycle, native batch place/cancel, perpetual
amend, listenKey private stream subscription, heartbeat helpers, public depth
parser, private order/fill/balance/position parser, request signing, error
classification, and offline request-spec/parser tests.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST + private REST |
| USDT-M perpetual | `Perpetual` | Public REST + private REST |
| Testnet | n/a | No dedicated external Toobit sandbox is configured |

REST base URL: `https://api.toobit.com`

WebSocket base URL: `wss://stream.toobit.com/quote/ws/v1`

## Authentication

Private requests use Toobit Binance-style query signing:

- header: `X-BB-APIKEY`
- query fields: `timestamp`, `recvWindow`, `signature`
- signature: HMAC-SHA256 hex over the sorted URL-encoded query string

## Endpoint Mapping

| Standard capability | Spot endpoint | USDT-M endpoint |
| --- | --- | --- |
| symbol rules | `GET /api/v1/exchangeInfo` (`symbols`) | `GET /api/v1/exchangeInfo` (`contracts`) |
| order book | `GET /quote/v1/depth` | `GET /quote/v1/depth` |
| balances | `GET /api/v1/account` | `GET /api/v1/futures/balance` |
| positions | Spot has no position model | `GET /api/v1/futures/positions` |
| fees | config override only | `GET /api/v1/futures/commissionRate` |
| place order | `POST /api/v1/spot/order` | `POST /api/v2/futures/order` |
| cancel order | `DELETE /api/v1/spot/order` | `DELETE /api/v2/futures/order` |
| batch place | `POST /api/v1/spot/batchOrders` | `POST /api/v2/futures/batch-orders` |
| batch cancel | `DELETE /api/v1/spot/cancelOrderByIds` | `DELETE /api/v1/futures/cancelOrderByIds` |
| cancel all | `DELETE /api/v1/spot/openOrders` | `DELETE /api/v2/futures/batch-orders` |
| amend | Unsupported | `POST /api/v2/futures/order/update` |
| query order | `GET /api/v1/spot/order` | `GET /api/v2/futures/order` |
| open orders | `GET /api/v1/spot/openOrders` | `GET /api/v2/futures/open-orders` |
| fills | `GET /api/v1/account/trades` | `GET /api/v2/futures/user-trades` |
| public WS | `depth`, `trade`, `markPrice` subscribe specs | same |
| private WS | `POST /api/v1/userDataStream`, `/api/v1/ws/<listenKey>` | same listenKey route/parser |

## Official WebSocket Order Book Detail

P9 official verification confirms Spot and USDT-M public WS use
`wss://stream.toobit.com/quote/ws/v1`. The `depth` topic is a 300ms snapshot
stream when the book changes and carries 300 bids/asks per side. `diffDepth`
pushes changed levels every second. USDT-M also documents `wholeBookTicker` for
BBO. Payload version `v` is not guaranteed unique, so REST `/quote/v1/depth`
snapshot fallback is required on gaps.
`endpoint_mapping.yaml` records `depth`, `diffDepth`, `wholeBookTicker`, 300ms
snapshot cadence, 1s diff cadence, depth levels 300, `v` risk, and REST fallback.

## Official Position Detail

仓位接口核验见 [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)。Toobit USDT-M `GET /api/v1/futures/positions` 已由当前项目 `get_positions` runtime 和 private tests 覆盖；Spot 没有标准仓位模型。

## Explicit Boundaries

Spot quote-sized market order, Spot amend, OCO/OTO order lists, a verified Spot
fee endpoint, USDT-M position-mode switch, countdown cancel-all, funding WS, and
a dedicated external testnet are not advertised as supported.

## Toolchain Metadata

- `capabilities_v2` declares read-only/trade credential scopes, signed endpoint
  metadata, cursor/limit order and fill history, native partial batch support,
  and listenKey private stream runtime behavior.
- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/toobit/endpoint_mapping.yaml`
  records REST/WS operation coverage, auth class, rate-limit bucket, native
  batch/atomicity, request-spec status, parser fixture, pagination, and
  reconciliation metadata.
- Rate limits are modeled as public, private-read, and trade buckets. Native
  batch place/cancel advertise max 20 items and partial success semantics.
- Private streams use `POST /api/v1/userDataStream` listenKey auth and
  `PUT /api/v1/userDataStream` keepalive renewal. Metadata declares client
  heartbeat, reconnect login, resubscribe, and order-book snapshot resync.
- Reconciliation after reconnect uses REST depth snapshots for public books and
  REST open-order/fill/account readback for private state.
- Live-dry-run promotion requires the shared runner controls: reconciliation
  enabled, kill-switch active, disabled-symbol filtering, and max-notional
  limits.

## Validation

- `CARGO_TARGET_DIR=target/toobit-task-check cargo check -p rustcta-exchange-gateway --lib`
- `CARGO_TARGET_DIR=target/toobit-task-check cargo test -p rustcta-exchange-gateway toobit --lib`
- `python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/toobit/endpoint_mapping.yaml`

## Fee Boundary

费率来源在官方/account/config 资料中存在；当前已补 futures `GET /api/v1/futures/commissionRate` 的 `request_specs/get_fees_commission_rate.json` 离线 request-spec 边界，并用 `request_specs/get_fees_spot_source_boundary.json` 记录 Spot 配置型 fee source fallback。shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 futures parser、spot fallback source policy 和 credential guard。
