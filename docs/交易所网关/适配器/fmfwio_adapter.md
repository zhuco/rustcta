# FMFW.io Adapter

Scope: A-20 `fmfwio`, HitBTC-family FMFW.io profile.

Runtime status:

- Implemented: spot public REST symbol rules and order book snapshots.
- Offline only: HS256 private REST request specs, signing vectors, WS subscribe/unsubscribe/auth payloads, and parser fixtures.
- Unsupported at runtime: private trading, private readbacks, WebSocket supervisors, margin, futures, wallet transfer and withdrawal.

Official API sources:

| Area | Source |
| --- | --- |
| REST base URL | `https://api.fmfw.io/api/3` |
| Public WS | `wss://api.fmfw.io/api/3/ws/public` |
| Trading WS | `wss://api.fmfw.io/api/3/ws/trading` |
| Wallet WS | `wss://api.fmfw.io/api/3/ws/wallet` |
| Documentation | <https://api.fmfw.io/> |

Product line:

- `MarketType::Spot` only in runtime.
- FMFW.io documents margin and futures endpoints, but A-20 does not enable them because the task is an alias/profile audit and shared HitBTC-family core ownership belongs to parallel tasks.
- Official product-line conclusion: Margin/Futures/Perpetual are `项目未实现`, not `交易所不支持合约`.

官方核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。
官方 Futures/Perpetual/Margin 线索在当前 Spot profile 中属于 `项目未实现` 产品线边界，不能写成 `交易所不支持合约`。

2026-06-09 产品线边界收窄：`contract_product`、`margin_product` 和
`futures_product` 绑定
`tests/fixtures/exchanges/fmfwio/request_specs/product_line_source_boundary.json`。
Futures fee/source specs 只是产品线边界证据，不表示 FMFW.io spot profile 已接 futures
runtime；Margin/Futures/Perpetual 仍需 product-scope guard、metadata、positions/risk
parser、private order runtime 和 dry-run reconciliation。
状态建议：继续保留 `contract_product` / `margin_product` /
`futures_product = 项目未实现`；官方 Margin/Futures/Perpetual 线索不能写成
`不支持`，也不能由 Spot lifecycle/readback runtime 推断为 derivatives runtime。

Authentication:

- REST HS256 signs `<method> + <URL path> + [?query] + [body] + <timestamp> + [window]` with HMAC-SHA256.
- The `Authorization` header is `HS256 ` plus base64 of `api_key:signature:timestamp[:window]`.
- WS HS256 login signs `timestamp + window`.
- Fixtures use only `test-key` and `test-secret`.

Endpoint mapping:

- `crates/rustcta-exchange-gateway/src/adapters/fmfwio/endpoint_mapping.yaml`
- Public REST endpoints are native:
  - `GET /public/symbol`
  - `GET /public/orderbook/{symbol}`
- Core private endpoints `place_order`, `cancel_order`, `query_order`, `get_open_orders`, `get_recent_fills` plus advanced `amend_order` and `place_order_list` are wired to guarded signed REST runtime; balances/fees and private WS remain offline boundaries or runtime `Unsupported`.

Official Core Trading Detail:

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。FMFW.io 使用 HitBTC v3 family API，Spot order create/cancel/query/history 和 `client_order_id` 官方支持。

当前项目已把 Spot `place_order`、`cancel_order`、`query_order`、`get_open_orders`、`get_recent_fills` 接入 guarded HS256 private REST runtime。默认无 `FMFWIO_PRIVATE_REST_ENABLED` 与 API key/secret 时仍返回 guard error；mock REST tests 覆盖 signed path/header/body、order parser、open-order readback 和 recent fills reconciliation。`cancel_all_orders` 不在本 adapter 的 shared runtime 范围内。

Advanced order boundary:

| Capability | Current adapter status | Reason |
| --- | --- | --- |
| `amend_order` | guarded signed REST runtime | FMFW.io documents `PATCH /api/3/spot/order/{client_order_id}` replace semantics; request spec, HS256 vector and parser fixture are pinned, and mock REST tests cover body/header shape behind `FMFWIO_PRIVATE_REST_ENABLED` plus API key/secret guard. |
| `place_order_list` / OCO / OTO | guarded signed REST runtime | FMFW.io documents `POST /api/3/spot/order/list`; request spec, HS256 vector and contingency parser fixture are pinned, and mock REST tests cover signed order-list runtime behind explicit credential guard. |
| `batch_place_orders` | `unsupported` | No shared native batch place runtime is mapped. |
| `batch_cancel_orders` | `unsupported` audit boundary | FMFW.io documents `DELETE /api/3/spot/order` cancel-all/filter-by-symbol; request spec, query-aware HS256 vector and parser fixture are pinned for audit, but this endpoint cannot losslessly execute a shared cancel-list request and would over-cancel matching open orders. |

WebSocket boundary:

- Public order book subscribe/unsubscribe payloads are covered by tests.
- Server ping heartbeat policy is documented as 30 seconds.
- REST `get_order_book` is the order book resync fallback.
- Private WS login payload is covered, but no production private stream is enabled.

Official WebSocket order book detail:

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。FMFW.io 使用 HitBTC v3 family public WS，支持 `orderbook/full`、`orderbook/D5|D10|D20/{100ms|500ms|1000ms}` 和 `orderbook/top/{100ms|500ms|1000ms}`。

Partial orderbook 有 sequence `s`，top 是 L1/BBO；未见 checksum。YAML 已结构化记录 `orderbook/D5|D10|D20/{100ms|500ms|1000ms}`、`orderbook/top/{100ms|500ms|1000ms}`、`orderbook/full`、100ms 最快推流、1/5/10/20 档证据和 REST `/public/orderbook/{symbol}` 重建边界。当前 adapter 只保留 spec/parser 边界，生产 book-cache 仍需补 supervisor 与 sequence 连续性校验。

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/fmfwio/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway fmfwio --lib --message-format short
```

If app config wiring is touched:

```bash
cargo test -p rustcta-gateway fmfwio --message-format short
```

## Fee Boundary

FMFW.io/HitBTC-family fee endpoints 已记录为离线 request-spec 边界：

- Spot: `GET /api/3/spot/fee`，fixture `tests/fixtures/exchanges/fmfwio/request_specs/get_fees_spot.json`，HS256 signing vector `get_fees_spot_hs256`。
- Futures: `GET /api/3/futures/fee`，fixture `tests/fixtures/exchanges/fmfwio/request_specs/get_fees_futures.json`，HS256 signing vector `get_fees_futures_hs256`。

官方示例也允许 Basic `apiKey:secretKey` 读取 private fee endpoints；当前 gateway fixture 固定使用 HS256 以复用 HitBTC-family signer 口径。Parser 需把 `symbol`、`make_rate`、`take_rate` 归一到 maker/taker fee snapshots，并在 FMFW.io profile scope guard 通过后才可启用。当前 shared `get_fees` runtime 仍属于项目未实现/未启用，不能写成运行。

账户/余额已提升为 guarded runtime：FMFW.io/HitBTC-family `GET /api/3/spot/balance` 通过 `FMFWIO_PRIVATE_REST_ENABLED`、API key/secret 和 HS256 私有 REST 签名读取，parser 覆盖 `currency/available/reserved` 响应并按请求资产过滤。默认无凭据仍 fail-closed；提现、划转等资金移动继续在网关外。
