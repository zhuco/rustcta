# Cod3x Gateway Adapter

Task C-48 covers `cod3x`, a perpetual DEX candidate. This implementation is deliberately G0 audit-only: it registers the adapter and documents the capability boundary, but it does not promote production REST, GraphQL, WebSocket, or routed order signing.

Official sources reviewed:
- Cod3x docs: https://docs.cod3x.org/
- Cod3x website: https://www.cod3x.org/
- Cod3x app: https://app.cod3x.org/

The reviewed sources describe Cod3x as an AI perps trading terminal/routing layer with downstream venue profiles such as Hyperliquid, GMX V2, and Lighter. They do not establish a stable Cod3x-native exchange API with versioned market metadata, order book, account, order, signing, WebSocket, error-code, idempotency, or reconciliation contracts. Downstream venue APIs must remain their own adapters; this task does not copy or alias them into `cod3x`.

Product lines:
- Perpetual: `MarketType::Perpetual` profile candidate only; no live Cod3x-native runtime.
- Spot: 交易所不支持现货；未核验到 Cod3x-native 现货 exchange API。

Runtime boundary:
- Product: `MarketType::Perpetual`
- Venue profiles audited: Hyperliquid, GMX V2, Lighter
- Public WS: 交易所不支持公共 WS 行情（Cod3x-native profile 口径）；`endpoint_mapping.yaml` records `websocket.public.support: unsupported`, and downstream venue WebSocket 走各自 adapter
- Public REST: unsupported unverified
- Private reads: unsupported unverified
- Orders/cancels/amend/order-list/batch: explicit unsupported Cod3x-native profile boundary; downstream venue order semantics stay in their own adapters
- Account model: downstream venue account/wallet, not a Cod3x API-key/HMAC contract

Advanced order unsupported boundary:
- `amend_order`, `place_order_list`, `batch_place_orders`, `batch_cancel_orders`, and `cancel_all_orders` remain explicit unsupported Cod3x-native routing boundaries.
- `endpoint_mapping.yaml` maps each advanced operation to `/unsupported/cod3x/*` with `auth: unsupported`, `native_batch: false`, and `support: unsupported`; `tests/fixtures/exchanges/cod3x/unsupported_boundary.json` records the executable fixture evidence.
- `tests.rs` asserts legacy capability flags, `capabilities_v2` batch/cancel-all support, runtime `Unsupported.operation`, mapping support values, and the fixture boundary.
- Runtime is not promoted because no stable Cod3x-native advanced-order API, downstream venue selection contract, signing ownership, idempotency model, partial failure semantics, or post-write reconciliation contract is verified.

账户/余额接口写 `交易所不支持当前账户接口 runtime`：Cod3x-native profile 未建立稳定本地账户/余额 API；下游 venue account 继续走各自 adapter。

Core trading official detail:
- Source: [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)
- 写法：`交易所不支持当前交易/私有接口 runtime`。
- 原因：当前未建立稳定 Cod3x-native exchange API；下游 venue 的下单/撤单能力必须继续由各自 adapter 承接，不复制到 `cod3x`。

Position official detail:
- Source: [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)
- 写法：`交易所不支持当前仓位接口 runtime`。
- 原因：Cod3x 是 perps terminal/routing layer；下游 venue 的 positions 走各自 adapter，不并入 Cod3x-native profile。

Validation:
- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cod3x/endpoint_mapping.yaml`
- `cargo fmt --check --package rustcta-exchange-gateway`
- `cargo check -p rustcta-exchange-gateway --lib --message-format short`
- `cargo test -p rustcta-exchange-gateway cod3x --lib --message-format short`
- `cargo test -p rustcta-gateway cod3x --message-format short`

Do not run `cargo build` for this task.

## Fee Boundary

交易所不支持当前费率接口 runtime：Cod3x-native 不提供本地 fee runtime；费用属于下游 venue。
