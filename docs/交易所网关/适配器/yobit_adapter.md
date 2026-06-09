# YoBit Gateway Adapter

Status date: 2026-06-09

Adapter id: `yobit`

Implementation status: Spot adapter with public REST v3 support for pair
metadata and order-book snapshots plus credential-gated read-only TAPI runtime
for order/fill readbacks. Private writes, batch/order-list writes, balances,
fees, withdrawal/deposit address operations, Yobicode operations, Defi swap, and
WebSocket runtime remain deliberately disabled or offline until separately
validated.

## Official Materials

| Area | Source | Adapter use |
| --- | --- | --- |
| Public API | `https://www.yobit.net/en/api/` documents Public API v3 `info`, `ticker`, `depth`, and `trades`. | Adapter uses `GET /api/3/info` and `GET /api/3/depth/{pair}`. |
| Trade API | `https://www.yobit.net/en/api/` documents `/tapi/`, `Key` and `Sign` headers, HMAC-SHA512, nonce, `getInfo`, `Trade`, `ActiveOrders`, `OrderInfo`, `CancelOrder`, and `TradeHistory`. | `OrderInfo`, `ActiveOrders`, and `TradeHistory` are guarded runtime readbacks; writes and balances remain request-spec/offline. |
| API rules | `https://www.yobit.net/en/rules/` documents API use and a 100 requests/minute limit. | Endpoint mapping uses conservative 100/min buckets. |

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST G1 for symbol rules and order book. |
| Defi swap | n/a | Unsupported; not mapped to central limit order book semantics. |
| Deposit/withdraw/Yobicode | n/a | Unsupported and out of gateway trading scope. |
| Perpetual/futures | n/a | `交易所不支持合约`; P6 official verification found only spot/TAPI plus Defi swap/Yobicode surfaces. |
| Testnet | n/a | Unsupported; no stable public sandbox host verified. |

Default REST base URL: `https://yobit.net`

## Endpoint Mapping

| Gateway capability | YoBit endpoint | Current status |
| --- | --- | --- |
| Symbol rules | `GET /api/3/info` | Implemented for Spot parser fixtures. |
| Order book | `GET /api/3/depth/{pair}?limit={depth}` | Implemented for snapshot parser fixtures. |
| Balances | `POST /tapi/ method=getInfo` | Request-spec-only; runtime returns `Unsupported("yobit.balances_request_spec_only")`. |
| Place order | `POST /tapi/ method=Trade` | Request-spec-only; runtime returns `Unsupported("yobit.place_order_request_spec_only")`. |
| Cancel order | `POST /tapi/ method=CancelOrder` | Request-spec-only; runtime returns `Unsupported("yobit.cancel_order_request_spec_only")`. |
| Query order | `POST /tapi/ method=OrderInfo` | Guarded read-only runtime; requires `YOBIT_PRIVATE_REST_ENABLED`, TAPI key/secret, and `exchange_order_id`. |
| Open orders | `POST /tapi/ method=ActiveOrders` | Guarded read-only runtime; requires `YOBIT_PRIVATE_REST_ENABLED`, TAPI key/secret, and symbol scope. |
| Recent fills | `POST /tapi/ method=TradeHistory` | Guarded read-only runtime; requires `YOBIT_PRIVATE_REST_ENABLED`, TAPI key/secret, symbol scope, and context tenant/account ids. |
| Batch place/cancel | Not verified | Unsupported. |
| WebSocket | 交易所不支持公共 WS 行情 | 官方 API v2/v3/TAPI/Defi API 覆盖 REST market data 和交易接口，未见公共订单簿 WS；REST reconciliation fallback documented for future private promotion. |

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。YoBit Trade API 明确用于创建和取消订单，并支持 `Trade`、`ActiveOrders`、`OrderInfo`、`CancelOrder`、`TradeHistory`。

账户/余额接口 `POST /tapi/ method=getInfo` 已补 `get_balances` 离线 request-spec、TAPI HMAC 形状和响应样例；shared `get_balances` runtime 仍属未启用，剩 HMAC-SHA512 auth smoke、getInfo parser 和 account reconciliation，不能写成交易所不支持余额。

当前 private REST 仅提升 `OrderInfo`、`ActiveOrders`、`TradeHistory` 三个只读 readback runtime；运行时默认 fail closed，必须显式开启 `YOBIT_PRIVATE_REST_ENABLED` 并提供 TAPI key/secret。`Trade`、`CancelOrder`、批量/订单列表写侧仍为离线 private trading boundary，不是 live runtime，也不是 `交易所不支持下单/撤单`。后续若要提升写侧，仍需单独完成 Trade/CancelOrder parser、reconciliation、dry-run/live-write gate。

## Authentication

YoBit Trade API signs the URL-encoded POST body with HMAC-SHA512 and sends:

- `Key: <api key>`
- `Sign: <hex hmac sha512>`

The adapter includes `hmac_sha512_hex`, guarded runtime readback requests, and
sanitized fixtures:

- fixture: `tests/fixtures/exchanges/yobit/signing_vectors/rest_tapi_hmac_sha512.json`
- request specs: `tests/fixtures/exchanges/yobit/request_specs/*.json`

Private REST readbacks remain disabled by default even when credentials are
present. Enable them with `YOBIT_PRIVATE_REST_ENABLED=true` plus
`YOBIT_API_KEY`/`YOBIT_API_SECRET` or the `YOBIT_SPOT_` equivalents.

## Capability Boundary

Default `capabilities()` returns:

- `market_types = [Spot]`
- public REST, symbol rules, and order-book snapshots supported
- private REST readbacks, query order, open orders, and recent fills supported
- balances, fees, trading writes, cancel, batch, cancel-all, public streams, and
  private streams unsupported/offline
- `capabilities_v2.private_rest` native; readback endpoints native, write/balance
  endpoints remain request-spec/offline

This adapter must not promote write support until a separate validation task
confirms write request semantics without real order placement.

## Validation

Allowed validation for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/yobit/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway yobit --lib --message-format short
cargo test -p rustcta-gateway yobit --message-format short
```

Do not run `cargo build` for this task.

## Fee Boundary

费率来源在 public `/api/3/info` pair metadata 中存在，当前已记录离线 source boundary：`tests/fixtures/exchanges/yobit/request_specs/get_fees_source_boundary.json`。该 source 适用 Spot pair metadata，`fee` 字段需要明确单费率到 maker/taker 的映射策略和 symbol scope；如生产使用需固定 source version 或显式 override。shared `get_fees` runtime 仍属项目未实现/未启用，剩 public info fee parser、single-rate policy 和 `FeeRateSnapshot` 映射。
