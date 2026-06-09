# Paradex Gateway Adapter

Status: Task 8 offline-verifiable adapter for Paradex perp/options DEX scope.

## Scope

- Products: perpetuals and options.
- Spot: 项目未实现 Spot。Paradex 官方交易页列出 Spot Trading，当前 adapter 只接 perpetual/options scope。
- REST base URL: `https://api.prod.paradex.trade/v1`.
- WebSocket URL: `wss://ws.api.prod.paradex.trade/v1`.
- Public scope: markets, order book, public WebSocket subscription payloads.
- Private scope: auth/order/position/open-order request specs and Stark signing boundary fixtures.

Spot 边界写入 `spot_product status: project_unimplemented`：当前只接 perpetual/options markets/orderbook 和 JWT/Stark 私有边界。补 Spot 前需要 spot markets filter、spot orderbook、spot order/cancel/query/open/fills、spot balance/account reconciliation 和 parser fixtures。

## Signing And Sessions

Paradex private access is split into Stark-authenticated JWT creation and bearer-authenticated private REST/WS use. The adapter keeps both explicit:

- `/auth` uses `PARADEX-STARKNET-ACCOUNT`, `PARADEX-STARKNET-SIGNATURE`, `PARADEX-TIMESTAMP`, and `PARADEX-SIGNATURE-EXPIRATION`.
- Private REST uses `Authorization: Bearer ...`.
- Order placement includes an order-level Stark signature in the request body.

Fixtures use deterministic placeholder digests and `<fixture-stark-signature>` only. No wallet private key, session token, or chain account fixture is committed.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/paradex/endpoint_mapping.yaml`.

Covered operations:

- `symbol_rules`: `GET /markets`, adapter-specific option/perp metadata fixture.
- `order_book`: `GET /orderbook/{market}`.
- `auth`: `POST /auth`, signed request-spec fixture.
- `positions`: `GET /positions`, bearer request-spec fixture boundary.
- `place_order`: `POST /orders`, offline request-spec only.
- `cancel_order`: `DELETE /orders/{order_id}`, offline request-spec only.
- `open_orders`: `GET /orders`, bearer request-spec boundary.

## Capability Boundary

The runtime adapter advertises public REST and public WS specs. Private reads are gated on JWT/Stark credentials. Private writes remain disabled at runtime and return explicit `Unsupported` until a separate live-dry-run promotion validates production Stark signing, account permissions, and reconciliation behavior.

账户/余额已补离线边界：Paradex `GET /balance` 和 private WS `user.balance` 已固定 `request_specs/get_balances.json`，矩阵按 `get_balances=离线` 记录；当前 shared `get_balances` runtime 尚未接 JWT/Stark auth guard、balance parser 和 REST/WS reconciliation。

Options metadata is adapter-specific in `tests/fixtures/exchanges/paradex/markets.json`; no shared trait was expanded.

WebSocket fixtures cover subscribe, unsubscribe, heartbeat ping/pong, private auth, and private order/position parser samples. Private stream disconnects require REST reconciliation over positions, open orders, and fills before live promotion.

Official public order book WS uses
`order_book.{market_symbol}.{feed_type}@15@{refresh_rate}` on
`wss://ws.api.prod.paradex.trade/v1`. The channel is fixed at 15 levels,
supports feed types `snapshot`, `deltas`, and `interactive`, and allows
`refresh_rate` 50ms or 100ms. Messages include `seq_no` and update arrays; no
checksum was found. The BBO channel remains a separate 1-level feed. Current
project support is native and mapping records 50/100ms, 15 levels, feed_type,
`seq_no`, BBO 1 level, and resubscribe/rebuild fields. Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

Unsupported or deferred:

- Live order placement/cancel.
- Native batch place/cancel.
- Cancel-all and amend semantics.
- Shared option Greeks/risk model.

## Validation

Allowed targeted commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/paradex/endpoint_mapping.yaml
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/paradex/*.rs
cargo test -p rustcta-exchange-gateway paradex --lib --message-format short
```

Latest local result: endpoint mapping validation and `rustfmt --check` pass. The focused cargo test was attempted with `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/paradex-task-check`, but compilation currently fails in unrelated in-progress shared registry entries and other adapters before Paradex tests can run. `cargo build` was not run.

## Fee Boundary

交易所不支持当前费率接口 runtime：fee 未映射到 shared trait，private JWT/Stark gated。

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. Paradex Spot Trading exists as a separate product line, while this adapter is scoped to perpetual/options markets, order book, and JWT/Stark private order/account boundaries.

Do not promote Spot runtime from options/perpetual market filters or Stark order builders. Promotion requires spot market filters, spot order-book public parser, spot balance private specs, spot order lifecycle/orders/open-orders/fills private specs, JWT/Stark product-scope auth, and REST/WS reconciliation.
