# HitBTC Adapter

Scope: A-23 `hitbtc`, canonical HitBTC v3 family adapter.

Runtime status:

- Implemented: spot public REST symbol rules and order book snapshots.
- Offline only: HS256 private REST request specs, signing vectors, WS subscribe/unsubscribe/auth payloads, and parser fixtures.
- Unsupported at runtime: private trading, private readbacks, WebSocket supervisors, margin, futures, wallet transfer and withdrawal.

Official API sources:

| Area | Source |
| --- | --- |
| REST base URL | `https://api.hitbtc.com/api/3` |
| Public WS | `wss://api.hitbtc.com/api/3/ws/public` |
| Trading WS | `wss://api.hitbtc.com/api/3/ws/trading` |
| Wallet WS | `wss://api.hitbtc.com/api/3/ws/wallet` |
| Documentation | <https://api.hitbtc.com/> |

Product line:

- `MarketType::Spot` only in runtime.
- HitBTC v3 documents spot, margin, futures and wallet endpoints. Futures/Margin derivatives are 项目未实现 in this spot runtime, not `交易所不支持合约`. Wallet transfers and withdrawals stay outside trading runtime scope.
- 2026-06-09 boundary update: `contract_product`, `margin_product` and
  `futures_product` are fixture-backed by
  `tests/fixtures/exchanges/hitbtc/request_specs/product_line_source_boundary.json`.
  Futures fee/source specs are product-line boundary evidence only; they do not
  make the guarded Spot adapter a futures runtime. Futures/Margin require
  separate product-scope guards, metadata, positions/risk parsers and private
  order reconciliation.
  Status recommendation: keep `contract_product`, `margin_product` and
  `futures_product` as `project_unimplemented`; do not mark official HitBTC
  margin/futures surfaces as exchange-unsupported, and do not promote them from
  Spot lifecycle/readback runtime.

Authentication:

- REST HS256 signs `<method> + <URL path> + [?query] + [body] + <timestamp> + [window]` with HMAC-SHA256.
- The `Authorization` header is `HS256 ` plus base64 of `api_key:signature:timestamp[:window]`.
- WS HS256 login signs `timestamp + window`.
- Fixtures use only `test-key` and `test-secret`.

Endpoint mapping:

- `crates/rustcta-exchange-gateway/src/adapters/hitbtc/endpoint_mapping.yaml`
- Public REST endpoints are native:
  - `GET /public/symbol`
  - `GET /public/orderbook/{symbol}`
- Core private endpoints `place_order`, `cancel_order`, `query_order`, `get_open_orders`, `get_recent_fills` plus advanced `amend_order` and `place_order_list` are wired to guarded signed REST runtime; balances/fees and private WS remain offline boundaries or runtime `Unsupported`.
- Fixture coverage includes public REST samples, private write/read request specs, HS256 signing vectors, P4 advanced-order parser samples, WS parser sample, and `unsupported_boundary.json`.
- P4 advanced order boundaries are machine-readable: `amend_order` and
  `place_order_list`/OCO/OTO/OTOCO are guarded HS256 private REST runtime
  paths with mock REST tests; `batch_cancel_orders` is an unsupported audit
  boundary because `DELETE /spot/order` is cancel-all/filter-by-symbol and
  would over-cancel a shared cancel-list request. Generic `batch_place_orders`
  remains unsupported because HitBTC single create and order-list semantics have
  no lossless mapping for arbitrary shared batch-place items; no generic batch
  runtime is enabled.

Official Core Trading Detail:

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。HitBTC v3 Spot 支持 `POST /api/3/spot/order`、`DELETE /api/3/spot/order`、查单、open/history/trade；支持 limit、market、stop/take-profit、GTC/IOC/FOK 和 `client_order_id`。

当前项目已把 Spot `place_order`、`cancel_order`、`query_order`、`get_open_orders`、`get_recent_fills` 接入 guarded HS256 private REST runtime。默认无 `HITBTC_PRIVATE_REST_ENABLED` 与 API key/secret 时仍返回 guard error；mock REST tests 覆盖 signed path/header/body、order parser、open-order readback 和 recent fills reconciliation。`cancel_all_orders` 不在本 adapter 的 shared runtime 范围内。

账户/余额 wallet/account readback 已提升为 guarded runtime：`GET /api/3/spot/balance` 通过 `HITBTC_PRIVATE_REST_ENABLED`、API key/secret 和 HS256 私有 REST 签名读取，parser 覆盖 `currency/available/reserved` 响应并按请求资产过滤。默认无凭据仍 fail-closed；提现、划转等资金移动继续在网关外。

高级订单能力按 P4 台账收口为显式边界：HitBTC v3 官方有 `PATCH /api/3/spot/order/{client_order_id}` replace、`POST /api/3/spot/order/list` order-list 和 `DELETE /api/3/spot/order` cancel-all/filter-by-symbol 语义。当前 `amend_order` 与 `place_order_list` 已接 guarded HS256 private REST runtime，默认无 `HITBTC_PRIVATE_REST_ENABLED` 与 API key/secret 时仍返回 guard error，mock REST 覆盖 signed request body/header 与 parser fixture；`batch_cancel_orders` 是 unsupported audit boundary，因为该 endpoint 是按 symbol/filter 撤全部订单，不能无损执行 shared cancel-list 语义，直接映射会 over-cancel 非请求 id 的 open orders。generic native `batch_place_orders` 暂无 lossless shared mapping，继续返回 `Unsupported`，且未启用 generic batch runtime。

WebSocket boundary:

- Public order book subscribe/unsubscribe payloads are covered by tests.
- Server ping heartbeat policy is documented as 30 seconds.
- REST `get_order_book` is the order book resync fallback.
- Private WS login payload is covered, but no production private stream is enabled.

Official WebSocket order book detail:

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。HitBTC v3 public WS 支持 `orderbook/full`、`orderbook/D5|D10|D20/{100ms|500ms|1000ms}` 和 `orderbook/top/{100ms|500ms|1000ms}`。

Partial orderbook 有 sequence `s`，top 是 L1/BBO；未见 checksum。YAML 已结构化记录 `orderbook/D5|D10|D20/{100ms|500ms|1000ms}`、`orderbook/top/{100ms|500ms|1000ms}`、`orderbook/full`、100ms 最快推流、1/5/10/20 档证据和 REST `/public/orderbook/{symbol}` 重建边界。当前 adapter 只保留 spec/parser 边界，生产 book-cache 仍需补 supervisor 与 sequence 连续性校验。

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/hitbtc/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway hitbtc --lib --message-format short
```

If app config wiring is touched:

```bash
cargo test -p rustcta-gateway hitbtc --message-format short
```

## Fee Boundary

HitBTC v3 Spot/Futures trading commission endpoints 已记录为离线 request-spec 边界：

- Spot: `GET /api/3/spot/fee`，fixture `tests/fixtures/exchanges/hitbtc/request_specs/get_fees_spot.json`，HS256 signing vector `get_fees_spot_hs256`。
- Futures: `GET /api/3/futures/fee`，fixture `tests/fixtures/exchanges/hitbtc/request_specs/get_fees_futures.json`，HS256 signing vector `get_fees_futures_hs256`。

官方示例也允许 Basic `apiKey:secretKey` 读取 private fee endpoints；当前 gateway fixture 固定使用 HS256。Parser 需把 `symbol`、`make_rate`、`take_rate` 归一到 maker/taker fee snapshots，并按 Spot/Futures product scope guard 控制是否启用。当前 shared `get_fees` runtime 仍属于项目未实现/未启用，不能写成运行。
