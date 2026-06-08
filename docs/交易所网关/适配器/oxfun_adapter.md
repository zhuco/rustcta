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
has a 100ms channel update frequency. Clients subscribe with
`{"op":"subscribe","args":["depthUpdate:BTC-USD-SWAP-LIN"]}`, wait for the
`depthUpdate` snapshot, then apply `depthUpdate-diff` absolute-quantity updates
by increasing `seqNum`; payloads include `checksum`.

## Official Position Detail

仓位接口核验见 [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)。当前 adapter 仅保留离线 spec/parser；private account/orders/positions/funding 仍未验证，`get_positions` 保持 unsupported。

写法：`交易所不支持当前仓位接口 runtime`。只有在官方 private positions endpoint、签名权限、分页/字段和 live auth 全部验证后，才重新核验是否补 runtime。

## Capability Boundary

Unsupported or spec-only:

- Private REST account/orders/positions/funding: unverified.
- Private WebSocket runtime: unverified; request-spec and parser fixtures only.
- Native batch place/cancel: documented over WebSocket, but runtime disabled.
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
