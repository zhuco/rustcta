# BSX Gateway Adapter

## Scope

- Adapter id: `bsx`
- Product line: Base perpetuals
- Current capability: public REST `products` and order book snapshot; guarded private REST readbacks for `query_order`, `get_open_orders`, and `get_recent_fills`; private REST/WS writes and account/fee/position reads are request-spec-only or Unsupported.
- Default trading posture: disabled. No live private write path is enabled.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Current adapter scope; public products and order-book snapshot are mapped. |
| Spot | n/a | 项目未实现 Spot。BSX 官方有 spot/perpetuals exchange 线索，当前 `bsx` adapter 只接 Base perpetuals，不能写成交易所不支持。 |

Spot 边界写入 `endpoint_mapping.yaml` 的 `spot_product status: project_unimplemented`：当前只用 `/products` 和 `/products/{product_id}/book` 做 perpetual metadata/book。补 Spot 前需要单独核验 spot products、spot order book、spot account/balance、spot order/open-order/fill lifecycle，并决定是否新增 BSX Spot profile。

## Official Materials

| Area | URL | Notes |
| --- | --- | --- |
| REST overview | https://api-docs.bsx.exchange/reference/rest-overview | Production REST host `https://api.bsx.exchange` |
| Products | https://api-docs.bsx.exchange/reference/get-products | Product metadata source for `get_symbol_rules` |
| Product book | https://api-docs.bsx.exchange/reference/get-product-book | Public order book snapshot source |
| Order write | https://api-docs.bsx.exchange/reference/post-order | Requires account headers and EIP-712 order signature |
| WebSocket overview | https://api-docs.bsx.exchange/reference/websocket-overview | Production WS host `wss://ws.bsx.exchange/ws` |

## Endpoint Mapping

The authoritative mapping lives in
`crates/rustcta-exchange-gateway/src/adapters/bsx/endpoint_mapping.yaml`.

| Gateway operation | BSX endpoint | Support | Fixture |
| --- | --- | --- | --- |
| `get_symbol_rules` | `GET /products` | native public REST | `tests/fixtures/exchanges/bsx/products.json` |
| `get_order_book` | `GET /products/{product_id}/book` | native public REST | `tests/fixtures/exchanges/bsx/orderbook.json` |
| `query_order` | `GET /orders/{order_id}` | guarded native private REST | `tests/fixtures/exchanges/bsx/request_specs/query_order.json`; `tests/fixtures/exchanges/bsx/order_success.json` |
| `get_open_orders` | `GET /orders` | guarded native private REST | `tests/fixtures/exchanges/bsx/request_specs/get_open_orders.json`; `tests/fixtures/exchanges/bsx/open_orders_success.json` |
| `get_recent_fills` | `GET /trades` | guarded native private REST | `tests/fixtures/exchanges/bsx/request_specs/get_recent_fills.json`; `tests/fixtures/exchanges/bsx/recent_fills_success.json` |
| `place_order` | `POST /orders` | request-spec-only / Unsupported runtime | `tests/fixtures/exchanges/bsx/request_specs/place_order_unsupported.json` |
| `cancel_order` | `DELETE /orders/{order_id}` | request-spec-only / Unsupported runtime | `tests/fixtures/exchanges/bsx/request_specs/cancel_order_unsupported.json` |
| `amend_order` | signed account scoped REST/WS source boundary | request-spec-only / parser-backed offline boundary | `tests/fixtures/exchanges/bsx/request_specs/amend_order_request_spec_only.json`; `tests/fixtures/exchanges/bsx/request_specs/amend_order_source_boundary.json`; `tests/fixtures/exchanges/bsx/parser/amend_order_ack.json` |
| `batch_cancel_orders` | signed account scoped cancel source boundary | request-spec-only / parser-backed offline boundary | `tests/fixtures/exchanges/bsx/request_specs/batch_cancel_orders_request_spec_only.json`; `tests/fixtures/exchanges/bsx/request_specs/batch_cancel_orders_source_boundary.json`; `tests/fixtures/exchanges/bsx/parser/batch_cancel_orders_ack.json` |
| `place_order_list` / `batch_place_orders` / cancel-all | account scoped REST/WS | unsupported shared advanced-order boundary | n/a |

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。BSX 官方 REST 支持 `POST /orders` 和 `DELETE /orders/{order_id}`，但订单需要 account headers 和 EIP-712 order signature。

因此下单/撤单是 `官方支持，项目未实现/未启用`。当前 request-spec-only 不可进入实盘；补交易接口前必须完成 account header 审计、EIP-712 signer、cancel runtime 和 dry-run guard。只读订单/成交回读已可在 `RUSTCTA_BSX_PRIVATE_REST_ENABLED` 加 API key/secret/wallet 后以 guarded runtime 启用。

## Official Advanced Order Detail

BSX 官方 API 目录和 orders channel 资料给出 cancel/update 线索；`amend_order_request_spec_only.json`、`batch_cancel_orders_request_spec_only.json`、对应 source-boundary fixture 已绑定到 signed `spec_only` endpoint，且 `parser/amend_order_ack.json`、`parser/batch_cancel_orders_ack.json` 覆盖改单 ack 与 batch cancel per-item 成败模型。矩阵按 `amend_order=离线`、`batch_cancel_orders=离线` 记录。当前没有启用 live private write runtime，也没有伪造 signer；剩余 account headers、EIP-712/session signing、cancel-v2 语义审计、partial-failure reconciliation runtime 和 dry-run guard。`place_order_list` 与 `batch_place_orders` 未核到可无损映射的 native shared 语义，继续保留 explicit unsupported boundary。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。BSX account/order 资料和 account-scoped private state 线索足以作为仓位或保证金状态 source boundary，但当前未 pin 稳定 positions REST path，也没有 position schema/parser、read-only permission scope 和 REST/WS reconciliation。

因此 `get_positions` 写 `项目未实现/离线边界`，绑定 `tests/fixtures/exchanges/bsx/request_specs/get_positions_source_boundary.json`，矩阵为 `get_positions=离线`。只有稳定 endpoint、BSX account headers、parser 和 reconciliation 完成后才可提升 runtime。

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。BSX public WS 主网为 `wss://ws.bsx.exchange/ws`，订单簿 channel 是 `book`，订阅 payload 为 `{"op":"sub","channel":"book","product":"BTC-PERP"}`。

`book` 订阅后先推 snapshot，之后有变化时每 25ms 推增量，并且每分钟重新推 snapshot。官方未给固定 depth/档位。payload 有 `timestamp` 和 `gsn`，未见 checksum；断线、乱序或 gap 时应重新订阅并用 REST `GET /products/{product_id}/book` 重建。

## Authentication And Signing

Public REST is unauthenticated. Private/account endpoints use BSX account headers such as
`BSX-KEY`, `BSX-SIGNATURE` and `BSX-TIMESTAMP`; websocket auth fixtures model an HMAC-SHA256
payload of `api_key,timestamp_ns`.

Order writes require an EIP-712 order signature over BSX order fields. The adapter records the
boundary in `tests/fixtures/exchanges/bsx/signing_vectors/eip712_order_boundary.json` but does not
construct live order signatures or submit private writes.

## Capability Boundary

- Market type is restricted to `perpetual`.
- `supports_public_rest`, `supports_symbol_rules` and `supports_order_book_snapshot` are enabled
  when public REST is enabled.
- `supports_private_rest`, `supports_query_order`, `supports_open_orders` and `supports_recent_fills`
  are enabled only when `RUSTCTA_BSX_PRIVATE_REST_ENABLED` plus API key/secret/wallet are configured.
- `supports_place_order`, `supports_cancel_order`,
  `supports_batch_place_order`, `supports_private_streams` and `supports_public_streams` remain
  false.
- Public websocket helpers generate fixture payloads only; no runtime connection/resync loop is
  enabled in this adapter.
- 费率项目未实现/未启用：BSX 已补 account/app `Fee Rate` source-boundary fixture，矩阵按 `get_fees=离线` 记录；仍需稳定 fee endpoint 或配置 source、account-scoped headers/permission audit、maker/taker parser 和 `FeeRateSnapshot` runtime 后才能打开共享 `get_fees`。
- 账户/余额已补离线边界：BSX account-scoped balance/equity source 已固定 `request_specs/get_balances_source_boundary.json`，矩阵按 `get_balances=离线` 记录；仍需稳定 endpoint path audit、account headers、permission scope、balance parser 和 private REST/WS reconciliation。
- 仓位已补离线边界：BSX account-scoped position/margin-state source 已固定 `request_specs/get_positions_source_boundary.json`，矩阵按 `get_positions=离线` 记录；仍需稳定 endpoint path audit、account headers、permission scope、position parser 和 private REST/WS reconciliation。
- 高级订单已补离线边界：`amend_order` 和 `batch_cancel_orders` 绑定 signed request/source fixtures、fixture-only parser 和 `capabilities_v2` runtime-disabled endpoint，矩阵按 `离线` 记录；不能提升 runtime 的代码级阻塞是 BSX account headers、EIP-712/session signing、safe non-overcancel cancel shape、sequence/dry-run guard 和 post-write/partial-failure reconciliation。
- Balances, positions, fees, cancel and order writes require a follow-up account-permission,
  source/parser and EIP-712/cancel semantics audit.

## Configuration

See `config/bsx_gateway_example.yml`. It is disabled by default and contains placeholder credential
keys only.

Relevant environment variables:

- `RUSTCTA_BSX_REST_BASE_URL`
- `RUSTCTA_BSX_PUBLIC_WS_URL`
- `RUSTCTA_BSX_API_KEY`
- `RUSTCTA_BSX_API_SECRET`
- `RUSTCTA_BSX_WALLET_ADDRESS`
- `RUSTCTA_BSX_SIGNER_ADDRESS`
- `RUSTCTA_BSX_PRIVATE_REST_ENABLED` defaults to false
- `RUSTCTA_BSX_PRIVATE_STREAMS_ENABLED` defaults to false

## Validation

Allowed validation commands for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bsx/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bsx --lib --message-format short
cargo test -p rustcta-gateway bsx --message-format short
```

Do not run `cargo build` for this task.
## P2 Core Trading Boundary (2026-06-09)

P2 readbacks `query_order`, `get_open_orders`, and `get_recent_fills` are guarded native private REST behind `RUSTCTA_BSX_PRIVATE_REST_ENABLED` plus API key/secret/wallet. P2 writes `place_order`, `cancel_order`, and `cancel_all_orders` remain offline/spec-only or unsupported shared semantics; runtime promotion is blocked on BSX account headers, EIP-712/session signing, safe cancel shape, reconciliation, and dry-run guard.

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. BSX has spot/perpetual exchange material, while this adapter is scoped to Base perpetual products from `/products` and `/products/{product_id}/book`.

Do not promote Spot/OpenBook runtime from the perpetual profile. Promotion requires separate spot products and order-book public specs, spot account/balance/open-order/fill private readback, spot order/cancel lifecycle, account-header and EIP-712 signing audit, and reconciliation guards.
