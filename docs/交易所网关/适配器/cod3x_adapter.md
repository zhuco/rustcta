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
- Public WS: 交易所不支持公共 WS 行情（Cod3x-native profile 口径）；下游 venue WebSocket 走各自 adapter
- Public REST: unsupported unverified
- Private reads: unsupported unverified
- Orders/cancels/batch: unsupported unverified
- Account model: downstream venue account/wallet, not a Cod3x API-key/HMAC contract

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
