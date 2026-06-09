# ApeX Gateway Adapter

Status date: 2026-06-08

Adapter id: `apex`

Task 7 status: ApeX Omni perpetual-first public REST G1 plus guarded private
readback runtime for query/open/fills. Private order placement remains
unsupported because ApeX order creation requires zkLink/L2 payload signatures in
addition to API-key HMAC headers.

## Product Lines

| Product | MarketType | Current status |
| --- | --- | --- |
| Omni perpetual | `Perpetual` | Primary Task 7 scope; public symbol-rules/order-book REST implemented. |
| Spot | n/a | 项目未实现 Spot。ApeX Omni 官方 API 已出现 Spot account/wallet 线索，当前 `apex` adapter 只接 perpetual，不能写成交易所不支持。 |
| RWA / prediction / transfer / withdrawal | n/a | Unsupported; outside trading runtime scope. |

Spot 边界写入 `spot_product status: project_unimplemented`：当前只接 Omni perpetual symbols/book 和私有 request-spec 边界。补 Spot 前需要 spot symbols/order book、spot account/wallet reads、spot place/cancel/query/open/fills lifecycle、zkLink/L2 signer 适配和 parser fixtures。

REST base URLs:

- Mainnet: `https://omni.apex.exchange`
- Testnet: `https://testnet.omni.apex.exchange`

The runtime endpoint paths include `/api/v3/...`; keep the configured base URL
host-only to avoid double-prefixing `/api`. The official SDK has a different QA
host constant, so testnet host selection must stay configurable.

WS base URLs:

- Public: `wss://quote.omni.apex.exchange/realtime_public?v=2&timestamp=<ts>`
- Private: `wss://quote.omni.apex.exchange/realtime_private?v=2&timestamp=<ts>`

## Authentication

Private REST requires:

- `APEX-SIGNATURE`
- `APEX-TIMESTAMP`
- `APEX-API-KEY`
- `APEX-PASSPHRASE`

The API-key signature message is:

`timestamp + method + path + dataString`

GET requests sign the path/query. POST requests sign an alphabetically sorted
form body. The HMAC-SHA256 output is Base64 encoded.

Orders, transfers, and withdrawals require an additional zkKeys/L2 signature in
the request body. The gateway does not implement that signer yet, so private
writes are not promoted.

## Endpoint Mapping

Mapping file:
`crates/rustcta-exchange-gateway/src/adapters/apex/endpoint_mapping.yaml`

Implemented for runtime REST:

- `GET /api/v3/symbols`
- `GET /api/v3/depth`
- `GET /api/v3/open-orders`
- `GET /api/v3/order`
- `GET /api/v3/fills`

Covered as private request-spec/signing fixtures only:

- `GET /api/v3/account`
- `GET /api/v3/account-balance`
- `POST /api/v3/delete-order`

`POST /api/v3/order` is documented but unsupported by the adapter until
deterministic zkLink/L2 order signing vectors exist in Rust.

Native amend and batch place/cancel endpoints were not found in the audited
docs and remain unsupported.

## Official Core Trading Detail

P0 core-trading verification confirms ApeX Omni officially supports
`POST /v3/order`, `POST /v3/delete-order`, `GET /v3/order`, and
`GET /v3/open-orders`. Supported TIF values include `GOOD_TIL_CANCEL`,
`FILL_OR_KILL`, and `IMMEDIATE_OR_CANCEL`; `clientOrderId` is supported.
Market orders still require a price or worst-price because the order payload is
part of the zkLink signature. Therefore current `place_order` is `项目未实现`
pending a deterministic Rust zkLink/L2 signer, not `交易所不支持下单`.
核心交易项目未实现/未启用：ApeX 官方支持下单、撤单和订单 readback；当前
`query_order`、`get_open_orders`、`get_recent_fills` 已接 guarded private REST。
下单/撤单/cancel-all 仍缺 zkLink/L2 signer 和 dry-run guard，不能写成交易所不支持。

## WebSocket

Public WS uses payloads such as:

```json
{"op":"subscribe","args":["orderBook200.H.BTCUSDT"]}
```

Heartbeat uses client `ping` roughly every 15 seconds.

Official order book channel is `orderBook{limit}.H.{symbol}` on
`wss://quote.omni.apex.exchange/realtime_public`; `limit` supports 25 or 200.
The docs label this as high-frequency but do not give a fixed millisecond
interval. The feed is snapshot plus delta; official guidance is to apply updates
in timestamp/ID order and rebuild from snapshot on disorder or gaps. No checksum
was found. Current project support is native and mapping records
`orderBook25`/`orderBook200`, no fixed ms, 25/200 depth, snapshot+delta, and
snapshot rebuild on disorder or gaps. Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

Private WS logs in with an `op = "login"` message whose `args` contains a JSON
string with API key fields, signature, and `ws_zk_accounts_v3` topics. Runtime
private WS routing remains disabled until auth and reconciliation are tested.

## Fixtures

Fixtures live under `tests/fixtures/exchanges/apex/`:

- `symbols_perpetual.json`
- `depth_btcusdt.json`
- `order_success.json`
- `open_orders_success.json`
- `recent_fills_success.json`
- `account_positions.json`
- `request_specs/get_open_orders.json`
- `request_specs/get_account_balance.json`
- `request_specs/get_account.json`
- `request_specs/query_order.json`
- `request_specs/get_fills.json`
- `request_specs/cancel_order.json`
- `request_specs/cancel_all_orders.json`
- `request_specs/place_order_unsupported_zk.json`
- `signing_vectors/private_get_open_orders.json`
- `ws_public_orderbook.json`
- `ws_private_login.json`
- `unsupported_boundary.json`

## Unsupported Boundary

The current adapter is not live-trade-enabled. `place_order` returns
`Unsupported("apex.place_order_requires_zklink_l2_signature")`. Market orders
also require a signed price/worst-price field and are unsupported.

Before write promotion, add a Rust zkLink/L2 signer with deterministic official
vectors and live-trading dry-run guard.

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/apex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway apex --lib --message-format short
```

## Fee Boundary

交易所不支持当前费率接口 runtime：未核到可映射成共享 FeeRateSnapshot 的稳定 private fee-tier endpoint。
## P2 Core Trading Boundary (2026-06-09)

P2 core trading is guarded runtime for `query_order`, `get_open_orders`, and
`get_recent_fills` when `APEX_PRIVATE_REST_ENABLED`/`RUSTCTA_APEX_PRIVATE_REST_ENABLED`
plus API key, API secret, and passphrase are configured. Place/cancel/cancel-all
remain offline because they are private writes and require deterministic
zkLink/L2 signer coverage.

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. ApeX has spot account/wallet and trading clues, while this adapter is scoped to ApeX Omni perpetual public REST plus private request-spec boundaries.

Do not promote Spot runtime from the perpetual profile. Promotion requires spot symbol and order-book public specs, spot wallet/account/balance private readback, spot place/cancel/query/open/fill lifecycle, zkLink/L2 signing scope, and reconciliation guards.
