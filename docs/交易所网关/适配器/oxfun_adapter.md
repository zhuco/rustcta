# OX.FUN Gateway Adapter

Adapter id: `oxfun`

Status: Task 10 audit shell, offline-verifiable only. The adapter records OX.FUN
perpetual/options API boundaries, HMAC signing vectors, WebSocket subscription
payloads, heartbeat policy, and parser fixtures. It does not enable live private
trading or production stream runtime.

## Official Materials

| Topic | Source |
| --- | --- |
| API overview | <https://oxoxox.gitbook.io/ox-docs/> |
| WebSocket authentication | <https://oxoxox.gitbook.io/ox-docs/api/websocket/authentication> |
| Public market channel | <https://oxoxox.gitbook.io/ox-docs/api/websocket/subscriptions-public/market> |
| Incremental book resync | <https://oxoxox.gitbook.io/ox-docs/api/websocket/subscriptions-public/incremental-order-book> |
| Place order command | <https://oxoxox.gitbook.io/ox-docs/api/websocket/order-commands/place-limit-order> |
| REST HMAC examples | <https://oxoxox.gitbook.io/ox-docs/api/restapi/deposits-and-withdrawals-private> |

## Product Scope

- Declared market types: `MarketType::Perpetual`, `MarketType::Option`.
- Spot: 交易所不支持现货。OX.FUN 官方主线是 perpetual swaps/options-style scope，未见共享 Spot 订单生命周期。
- OX.FUN documents WebSocket market metadata, depth diff/snapshot flow, and
  authenticated WebSocket order commands. The current adapter keeps these as
  parser/request-spec fixtures until a live resync task verifies sequencing and
  recovery.
- Public REST symbol/order-book endpoints were not confirmed from official docs,
  so `get_symbol_rules` and `get_order_book` return explicit `Unsupported`
  instead of synthesizing REST behavior from WebSocket fixtures.

## Signing

REST private examples sign:

```text
timestamp + "\n" + nonce + "\n" + METHOD + "\n" + host + "\n" + path + "\n" + body_or_query
```

with HMAC-SHA256 and base64 output. WebSocket login signs
`timestamp + "GET/auth/self/verify"`.

Sanitized vectors:

- `tests/fixtures/exchanges/oxfun/signing_vectors/rest_hmac.json`
- `tests/fixtures/exchanges/oxfun/signing_vectors/ws_login.json`

## WebSocket Policy

- Public URL: `wss://api.ox.fun/v2/websocket`
- Private URL: `wss://api.ox.fun/v2/websocket`
- Test WebSocket URL: `wss://stgapi.ox.fun/v2/websocket`
- Heartbeat: client text frame `ping`, 20s interval, 45s timeout.
- Resync requirement: subscribe to `depthUpdate:<marketCode>`, wait for the
  `depthUpdate` snapshot, then apply `depthUpdate-diff` events by increasing
  `seqNum`. Runtime is disabled until live gap handling is verified.
  The official test WebSocket URL is documented, but this task does not enable
  live trading, private streams, or public stream runtime on either environment.

P9 official verification adds the missing structure: `depthUpdate:<marketCode>`
is a 100ms depth channel. Clients subscribe with
`{"op":"subscribe","args":["depthUpdate:BTC-USD-SWAP-LIN"]}`, wait for the
`depthUpdate` snapshot, then apply `depthUpdate-diff` absolute-quantity updates
by increasing `seqNum`; payloads include `checksum`.
`endpoint_mapping.yaml` records the channel, 100ms cadence, snapshot+diff model,
`seqNum`, checksum support, and the resubscribe-on-gap boundary.

## Official Position Detail

仓位接口核验见 [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)。OX.FUN private account/positions/funding 材料提供 position readback 线索，但 stable positions endpoint、签名权限、分页/字段、parser 和 live auth 都未完成 runtime 审计。

写法：`get_positions` 是 `项目未实现/离线边界`，绑定 `tests/fixtures/exchanges/oxfun/request_specs/get_positions_account_source.json`，矩阵为 `get_positions=离线`。只有在官方 private positions endpoint、read-only auth、parser 和 REST/private-WS reconciliation 全部验证后，才提升 runtime。

账户/余额接口写 `项目未实现/离线边界`：private account/funding 材料提供账户状态线索，`endpoint_mapping.yaml` 已将 `get_balances` 写成 `source://oxfun/private-account-equity` spec-only source boundary，并绑定 `tests/fixtures/exchanges/oxfun/request_specs/get_balances_account_source.json`。矩阵应为 `get_balances=离线`；balances/account equity readback 尚未接入 shared runtime、parser、auth smoke 或 reconciliation。

## Capability Boundary

Unsupported or spec-only:

- Private REST account/orders/positions/funding: unverified.
- Private WebSocket runtime: unverified; request-spec and parser fixtures only.
- Native batch place: documented over WebSocket and pinned by `ws_batch_place_orders.json`; `capabilities_v2` exposes the native/partial batch boundary and parser fixtures cover single and partial `placeorders` acks with reconciliation plans. The offline reconciliation report is `tests/fixtures/exchanges/oxfun/ws/private_batch_place_reconciliation_report.json`. Runtime stays disabled because the adapter lacks authenticated private WS session lifecycle, request tag/order-id reconciliation, dry-run guard, and REST/private-stream readback after WS writes.
- Native batch cancel: not promoted; current fixtures only cover single cancel order.
- Dead-man switch, advanced order lists, leverage/margin/position-mode mutation:
  not mapped.
- Options contract metadata is documented only as product scope; no shared option
  model is added in this task.

## Endpoint Mapping

Machine-readable mapping:

`crates/rustcta-exchange-gateway/src/adapters/oxfun/endpoint_mapping.yaml`

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/oxfun/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway oxfun --lib --message-format short
```

Do not run `cargo build` or live trading commands for this adapter.

## Fee Boundary

交易所不支持当前费率接口 runtime：private account/orders/funding 仍未验证，未核到稳定 fee endpoint。
