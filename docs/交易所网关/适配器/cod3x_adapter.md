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

Validation:
- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cod3x/endpoint_mapping.yaml`
- `cargo fmt --check --package rustcta-exchange-gateway`
- `cargo check -p rustcta-exchange-gateway --lib --message-format short`
- `cargo test -p rustcta-exchange-gateway cod3x --lib --message-format short`
- `cargo test -p rustcta-gateway cod3x --message-format short`

Do not run `cargo build` for this task.
