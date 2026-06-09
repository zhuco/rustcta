# HollaEx Adapter

Scope: A-24 `hollaex`, official HollaEx demo/API profile only.

Runtime status:

- Implemented: spot public REST symbol rules from `GET /v2/constants`, order book snapshots from `GET /v2/orderbook?symbol=...`, and guarded private readbacks for `query_order`, `get_open_orders`, and `get_recent_fills`.
- Offline only: HMAC-SHA256 private write request specs, signing vectors, WS subscribe/unsubscribe/auth payloads, and unsupported boundary fixtures.
- Unsupported at runtime: private writes, balances, fees, WebSocket supervisors, HollaEx operator/admin APIs, funds movement, and arbitrary white-label exchange profiles.

Official API sources:

| Area | Source |
| --- | --- |
| REST base URL | `https://api.hollaex.com/v2` |
| WebSocket URL | `wss://api.hollaex.com/stream` |
| Documentation | <https://apidocs.hollaex.com/> |

Product line:

- `MarketType::Spot` only in runtime.
- HollaEx is a white-label exchange framework. This adapter maps only the official `api.hollaex.com` demo/API surface and does not generate profiles for arbitrary HollaEx-based venues.
- Official P6 product-line verification found no standard futures/perpetual/options
  surface for this official demo/API profile, so standard contracts are
  `交易所不支持合约` for this adapter. Arbitrary HollaEx white-label venues require
  separate verification before they can be mapped.

Authentication:

- REST private requests use `api-key`, `api-expires`, and `api-signature` headers.
- The fixture signature payload is `METHOD + path_with_query + api_expires + json_body`.
- The signature algorithm is HMAC-SHA256 hex.
- Private WS auth is documented as HMAC query params over `CONNECT + /stream + api_expires`.
- Fixtures use only `test-key` and `test-secret`.

Endpoint mapping:

- `crates/rustcta-exchange-gateway/src/adapters/hollaex/endpoint_mapping.yaml`
- Native public REST:
  - `GET /v2/constants`
  - `GET /v2/orderbook?symbol={symbol}`
- Private readback endpoints are credential-gated runtime for the official profile; private write endpoints remain request-spec/offline boundaries.
- P4 advanced order boundaries are explicit `Unsupported`: `amend_order`, `place_order_list`/OCO/OTO, `batch_place_orders`, and `batch_cancel_orders` have no verified native mapping for the official demo/API profile.

Official Core Trading Detail:

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。HollaEx API 支持 `POST /v2/order`、`DELETE /v2/order`、`DELETE /v2/order/all`、`GET /v2/order`；user order 支持 limit/market，`meta.post_only` 和可选 stop price。

当前 adapter 只映射官方 demo/API profile。`query_order`、`get_open_orders` 和 `get_recent_fills` 已启用 credential-gated read-only runtime，必须配置 `HOLLAEX_PRIVATE_REST_ENABLED` 或 `RUSTCTA_HOLLAEX_PRIVATE_REST_ENABLED`，并提供 HMAC API key/secret。`place_order`、`cancel_order` 和 `cancel_all_orders` 继续写 `离线`，不是 live runtime，也不是 `交易所不支持下单/撤单`；任意白标 HollaEx venue 仍需单独核验。

账户/余额接口 `/user/balance` 已补 `get_balances` 离线 request-spec、HMAC signing fixture 和响应样例；shared `get_balances` runtime 仍属未启用，剩 HMAC read-only runtime、`/user/balance` parser、白标 profile guard 和 reconciliation，不能写成交易所不支持余额。

高级订单能力按 P4 台账收口为当前不支持：官方 demo/API profile 未映射 in-place amend、OCO/OTO/order-list、native batch place 或 native batch cancel；这些能力在 gateway runtime 中继续返回 `Unsupported`。

`tests/fixtures/exchanges/hollaex/unsupported_boundary.json` 记录了结构化 `advanced_order_boundaries`：`amend_order`、`place_order_list`（OCO/OTO）、`batch_place_orders` 和 `batch_cancel_orders` 均为 true unsupported；`cancel_all_orders` 仅保留 core trading audit request-spec，runtime 仍为 offline guard。此边界不启用 live write。

WebSocket boundary:

- Public order book subscribe/unsubscribe payloads use `orderbook:{symbol}`.
- Client ping heartbeat policy is 30 seconds.
- REST `get_order_book` is the public order book resync fallback.
- Private WS auth URL construction is covered by an offline fixture, but no production private stream is enabled.

Official WebSocket order book detail:

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。HollaEx public WS 支持 `orderbook` channel，也可按 symbol 订阅 `orderbook:xht-usdt`；实际 endpoint 是对应白标 exchange 的 `wss://<exchange>/stream`。

官方公开资料未给固定推流毫秒、固定 depth、sequence 或 checksum，矩阵边界记录为无固定 ms、无固定 depth、no sequence、no checksum。HollaEx 是白标框架，任意具体 venue 的实时盘口质量需要单独核验；当前 adapter 以 REST `/v2/orderbook` 作为重建源。

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/hollaex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway hollaex --lib --message-format short
```

If app config wiring is touched:

```bash
cargo test -p rustcta-gateway hollaex --message-format short
```

## Fee Boundary

交易所不支持当前费率接口 runtime：官方 demo/API profile 未见稳定 maker/taker trading fee-rate endpoint；任意白标 venue 费率不同。
