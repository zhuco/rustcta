# CEX.IO Gateway Adapter

Status date: 2026-06-08

## Scope

Adapter id: `cex`.

This adapter covers CEX.IO spot public REST at G1:

- `get_symbol_rules` via `GET /currency_limits`
- `get_order_book` via `GET /order_book/{symbol1}/{symbol2}/`
- offline REST and WebSocket signing/payload fixtures
- credential-gated private REST readbacks for `query_order`, `get_open_orders`, and `get_recent_fills`
- explicit `Unsupported` boundaries for private writes, fiat ledger operations, margin, futures and transfers
- Contract/Futures/Options: 交易所不支持合约（当前 CEX.IO Spot Trading API 口径）。

## Official Interfaces

| Surface | URL | Status |
| --- | --- | --- |
| Legacy REST API | `https://cex.io/api` | Used for public REST implementation |
| WebSocket API | `wss://ws.cex.io/ws/` | Payload and heartbeat helpers only |
| Spot Trading Public WS | `wss://trade.cex.io/api/spot/ws-public` | Officially supports `order_book_subscribe` snapshot and `order_book_increment`; 项目未实现公共 WS runtime |
| Private REST | JSON POST body with `nonce`, `key`, `signature` | Read-only order/fill readbacks require `CEX_PRIVATE_REST_ENABLED` plus API key/secret/user id |

The REST docs publish `currency_limits` and `order_book` public endpoints. Private REST uses HMAC-SHA256 over `nonce + user_id + api_key`; WebSocket auth signs `timestamp + api_key`.

## Capability

| Capability | Status | Notes |
| --- | --- | --- |
| Spot symbol rules | Native | Parsed from `currency_limits.data.pairs` |
| Spot order book snapshot | Native | `depth` query is bounded to 1-100 locally |
| Private order/fill readbacks | Native when guarded | `query_order`, `get_open_orders`, and `get_recent_fills` use signed JSON POST only when `CEX_PRIVATE_REST_ENABLED` and credentials/user id are configured |
| Private balances/fees | 离线边界 | Balance and fee request specs remain offline; parser/profile scope promotion is still pending |
| Private order writes | 离线边界 | Fixtures document place/cancel/cancel-all request shapes; adapter does not send signed writes |
| Batch place/cancel | Unsupported | CEX.IO private write runtime is request-spec-only and no safe native/shared batch mapping is enabled |
| Amend / order list | Unsupported | No shared in-place amend or OCO/OTO/order-list endpoint is mapped for this profile |
| WebSocket runtime | 项目未实现公共 WS runtime | `endpoint_mapping.yaml` declares `websocket.public.support: spec_only`; official Spot Trading WS has `order_book_subscribe` snapshot and event-driven `order_book_increment` with `seqId`, no fixed ms interval, and no production socket runtime |
| Fiat ledger, deposit, withdrawal, transfer | Unsupported | Not a trading runtime operation |
| Contract/Futures/Options | 交易所不支持合约 | Current public trading documentation is CEX.IO Spot Trading REST/WS. |

## Official Core Trading Detail

P0 core-trading verification confirms CEX.IO Spot Trading supports private
trading through REST and WebSocket APIs: `do_new_order`,
`do_cancel_my_order`, and `do_cancel_all_orders`. Spot Trading supports Market,
Limit, and Stop Limit orders; TIF includes GTC, IOC, and GTD, with Market
orders restricted to IOC. `clientOrderId` and `cancelRequestId` are part of the
official flow. Current project private writes are pinned as `离线`
request-spec boundaries, not live runtime, and not `交易所不支持下单/撤单`.
Read-only order/fill readbacks are enabled only behind `CEX_PRIVATE_REST_ENABLED`
plus API key, API secret, and user id.
The REST legacy boundary also records `place_order`, `cancel_order`,
`cancel_all_orders`, `get_order`, `open_orders`, and `archived_orders` as
request-spec-only endpoints for the shared core trading backlog.

账户/余额 private readback 已补 `/balance/` 离线 request-spec、HMAC body 形状和响应样例；shared `get_balances` runtime 仍属未启用，剩 private balance parser、HMAC auth smoke 和 fiat/trading scope guard，不能写成交易所不支持余额。

## Files

- Adapter: `crates/rustcta-exchange-gateway/src/adapters/cex/`
- Mapping: `crates/rustcta-exchange-gateway/src/adapters/cex/endpoint_mapping.yaml`
- Fixtures: `tests/fixtures/exchanges/cex/`
- Disabled config: `config/cex_gateway_example.yml`

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway cex --lib --message-format short
cargo test -p rustcta-gateway cex --message-format short
```

Do not run `cargo build` for this task.

## Fee Boundary

CEX.IO fee sources 已记录为离线 request-spec 边界：

- Spot Trading current fee: `POST /get_my_current_fee`，fixture `tests/fixtures/exchanges/cex/request_specs/get_my_current_fee.json`，需要 Read permission 和 Spot Trading profile guard。
- Legacy REST fallback: `POST /get_myfee/`，fixture `tests/fixtures/exchanges/cex/request_specs/get_myfee_legacy.json`，复用 `rest_private_hmac_sha256` signing vector。

`get_my_current_fee` 返回 `data.tradingFee` 中以 `BTC-USD` 这类 dash pair 为 key 的 percent；legacy `get_myfee` 返回 `ETH:USD` 这类 colon pair，以及 `buy`/`sell` taker 与 `buyMaker`/`sellMaker` maker 百分比。Parser 需完成百分比单位归一、pair normalize、maker/taker 选择和 profile guard。当前 shared `get_fees` runtime 仍属于项目未实现/未启用，不能写成运行。
