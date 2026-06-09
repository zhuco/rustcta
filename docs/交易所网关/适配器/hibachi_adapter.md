# Hibachi Gateway Adapter

Status date: 2026-06-08

Task: A-22 / AI-R22 from `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

## Product Scope

- Adapter id: `hibachi`
- Product: USDT-settled perpetual contracts
- Implemented runtime surface: public REST market metadata, public REST order book snapshot, public fee readback from exchange info.
- Offline-only surface: private write request specs and exchange-managed HMAC signing vector.
- Explicitly closed: real place/cancel/amend/batch/cancel-all, private REST read runtime, private WebSocket runtime, withdrawals/transfers, trustless/non-custodial signing runtime.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Current adapter scope; public metadata, order book snapshot and fee readback are mapped. |
| Spot | n/a | 交易所不支持现货。官方文档里的 spot/index price 是指数价格，不是现货订单生命周期。 |

## Official Sources

| Area | Source |
| --- | --- |
| Market REST | `https://hibachi-docs.redocly.app/marketapi/market` |
| Account/trade REST | `https://hibachi-docs.redocly.app/accountapi/trade` |
| Authorization | `https://hibachi-docs.redocly.app/api/authorization` |
| Signing | `https://hibachi-docs.redocly.app/api/signing` |
| SDK WS defaults | `hibachi-xyz` Python SDK `api_ws_market.py`, `api_ws_account.py`, `api_ws_trade.py` |

Observed base URLs:

- Market/data REST: `https://data-api.hibachi.xyz`
- Account/trade REST: `https://api.hibachi.xyz`
- Market WS: `wss://data-api.hibachi.xyz/ws/market`
- Account WS: `wss://api.hibachi.xyz/ws/account`
- Trade WS: `wss://api.hibachi.xyz/ws/trade`

No public testnet/sandbox URL was confirmed during this implementation.

## Endpoint Mapping

Machine-readable mapping: `crates/rustcta-exchange-gateway/src/adapters/hibachi/endpoint_mapping.yaml`.

Key endpoints:

- `GET /market/exchange-info`: contract metadata, increments, min order size, min notional, fee config.
- `GET /market/data/orderbook`: order book snapshot by `symbol`, `depth`, and `granularity`.
- `GET /market/data/prices`: mark/trade/spot price and funding estimate, mapped as spec-only.
- `GET /market/data/open-interest`: open interest, mapped as spec-only.
- `GET /trade/account/info`, `GET /trade/orders`, `GET /trade/order`, `GET /trade/account/trades`: audited private read endpoints, runtime closed by default.
- `POST/DELETE/PATCH /trade/order`, `DELETE/POST /trade/orders`: write endpoints kept request-spec only. P4 fixtures now pin `amend_order`, `batch_place_orders`, and `batch_cancel_orders` request/parser shapes without enabling live writes.

## Signing Boundary

Hibachi documents order signing using an account model with `accountId`, `nonce`, symbol/contract fields, and a `signature` field. The adapter includes an offline exchange-managed HMAC-SHA256 vector for the documented byte-packing shape:

- Fixture: `tests/fixtures/exchanges/hibachi/signing_vectors/exchange_managed_hmac.json`
- Request specs:
  - `tests/fixtures/exchanges/hibachi/request_specs/get_open_orders.json`
  - `tests/fixtures/exchanges/hibachi/request_specs/query_order.json`
  - `tests/fixtures/exchanges/hibachi/request_specs/get_recent_fills.json`
  - `tests/fixtures/exchanges/hibachi/request_specs/positions_source_boundary.json`
  - `tests/fixtures/exchanges/hibachi/request_specs/place_order_limit.json`
  - `tests/fixtures/exchanges/hibachi/request_specs/amend_order.json`
  - `tests/fixtures/exchanges/hibachi/request_specs/batch_place_orders.json`
  - `tests/fixtures/exchanges/hibachi/request_specs/batch_cancel_orders.json`
- Parser fixtures:
  - `tests/fixtures/exchanges/hibachi/parser/amend_order_ack.json`
  - `tests/fixtures/exchanges/hibachi/parser/batch_place_orders_ack.json`
  - `tests/fixtures/exchanges/hibachi/parser/batch_cancel_orders_ack.json`

Trustless/non-custodial signing and any key registration flow are not enabled in runtime. P4 write endpoints are `spec_only` request/parser boundaries in `endpoint_mapping.yaml`; the adapter still returns `hibachi.trade_write_request_spec_only` for single place and specific P4 blockers for amend/batch live write methods.

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。Hibachi 官方 trade REST 支持 `POST/DELETE/PATCH /trade/order` 和批量 orders endpoints。

因此下单/撤单/改单/批量下单/批量撤单不能写成 `交易所不支持`。`PATCH /trade/order`、`POST /trade/orders`、`DELETE /trade/orders` 只作为 request-spec/parser/mapping 线索，mapping 显式记录为 `status: project_unimplemented` 且 endpoint 为 `spec_only`；`amend_order` 还缺 shared price/full replace 字段，`batch_place_orders` 还缺 contract-id/scale signer，`batch_cancel_orders` 还缺 verified batch signature parser/reconciliation。补交易接口前必须完成 auth/signing runtime、private readback/reconciliation 和 live guard。order-list/OCO 仍按交易所不支持边界记录。

P2 核心 readback 已提升为 guarded `运行`：`query_order` 读取 `GET /trade/order`，`get_open_orders` 读取 `GET /trade/orders`，`get_recent_fills` 读取 `GET /trade/account/trades`。三者默认未配置私有读权限时返回 `hibachi.*` operation boundary；只有 `HIBACHI_PRIVATE_REST_ENABLED`、API key 和 account id 同时配置时才发出 bearer private REST 请求。当前不提升下单/撤单/cancel-all，也不支持 client-order-id 查询、游标分页或时间窗/成交 id filter。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。Hibachi 官方交易/风险资料明确有 positions、cross margin、subaccount isolated-like usage、risk panel 和 liquidation 语义。

因此仓位接口已提升为 guarded `运行`：`get_positions` 在 `HIBACHI_PRIVATE_REST_ENABLED`、API key 和 account id 同时配置时读取 `GET /trade/account/info`，用 `tests/fixtures/exchanges/hibachi/parser/account_info_positions.json` 覆盖 positions/risk parser；默认未配置私有读权限时仍返回 `hibachi.private_rest_disabled`，不伪造 live 成功。当前仍保留 `tests/fixtures/exchanges/hibachi/request_specs/positions_source_boundary.json` 作为 account/risk readback 和 account WS 对账来源边界，后续剩 account WS reconciliation 和更完整 liquidation/risk 字段映射。

账户/余额已补离线边界：Hibachi `/trade/account/info` 账户/余额/风险 readback 已固定 `request_specs/get_balances_account_info.json`，矩阵按 `get_balances=离线` 记录。当前 shared `get_balances` runtime 尚未接 auth smoke、balance/equity/risk parser、read-only guard 和 account WS reconciliation。

## WebSocket Boundary

The SDK exposes market/account/trade WS clients. This adapter only builds deterministic payloads and documents heartbeat/reconnect policy:

- Subscribe: `{ "method": "subscribe", "channel": "orderbook/{symbol}" }`
- Unsubscribe: `{ "method": "unsubscribe", "channel": "orderbook/{symbol}" }`
- Keepalive: `{ "method": "ping" }`
- Account stream URL shape: `wss://api.hibachi.xyz/ws/account?accountId={accountId}`

Production WS runtime, sequence-gap handling, checksum validation, auth renewal, and private reconciliation are follow-up work.

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。Hibachi SDK/公开资料有 market WS `orderbook/{symbol}` payload，market WS URL 为 `wss://data-api.hibachi.xyz/ws/market`。

本批未核到固定推流毫秒、WS 固定 depth、sequence 或 checksum，矩阵边界记录为无固定 ms、无固定 depth、no sequence、no checksum。REST `/market/data/orderbook` 支持 `depth` 和 `granularity`，应作为断线/异常重建源；当前 adapter 不提升生产 WS runtime。

## Capabilities

| Capability | Status | Notes |
| --- | --- | --- |
| `MarketType::Perpetual` | Native | Public metadata and book snapshot only. |
| Symbol rules | Native | From `futureContracts` in `/market/exchange-info`. |
| Order book snapshot | Native | From `/market/data/orderbook`. |
| Fees | Native public readback | Maker/taker defaults from `feeConfig`. |
| Query/open orders/fills | Guarded private readback | `HIBACHI_PRIVATE_REST_ENABLED` plus API key/account id required; unsupported filters fail closed. |
| Balances/positions | Existing guarded private readback | Unchanged in this batch; account WS reconciliation remains follow-up. |
| Place/cancel/amend/batch/cancel-all | Project-unimplemented runtime | Official endpoints exist; offline request-spec and signing vector only. |
| Public/private WS runtime | Unsupported | Payload fixtures and URLs only. |
| Client order id | Unsupported | Account API notes nonce/order id identifiers and says client-id support is future work. |
| Testnet | Unsupported/unverified | No public sandbox base URL confirmed. |

## Validation

Allowed commands for this adapter:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/hibachi/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway hibachi --lib --message-format short
cargo test -p rustcta-gateway hibachi --message-format short
```

Do not run `cargo build`, release builds, app/web builds, live order placement, live cancel, withdrawal, transfer, or long-running private streams for this task.
## P2 Core Trading Boundary (2026-06-09)

P2 core place/cancel/cancel-all/query/open/fills are offline/spec-only trade REST boundaries. Runtime promotion is blocked on account/signing lifecycle, contract-id/scale mapping, parsers, nonce/readback reconciliation, and live dry-run guard.
