# Mango Markets Gateway Adapter

Status date: 2026-06-08

Adapter id: `mango_markets`

Task: C-44 / AI-D44 from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

Implementation status: G0 scan-only audit adapter. Mango Markets v4 is an
on-chain Solana protocol with SDK-driven account and transaction flows, not a
conventional REST trading venue. This adapter records public metadata parsing
fixtures, Solana RPC request-spec shape, WebSocket account subscription payload
shape and explicit `Unsupported` boundaries for all private/account/trading
capabilities.

## Official Source Table

| Topic | Source | Adapter use |
| --- | --- | --- |
| Mango v4 repository | <https://github.com/blockworks-foundation/mango-v4> | Confirms Mango v4 is a monorepo containing the Solana program plus TS/Python clients; records program and group ids. |
| Mango v4 overview | <https://deepwiki.com/blockworks-foundation/mango-v4/1-overview> | Used to document the Solana on-chain protocol, group/account/perp market model and cross-margin risk boundary. |
| MangoClient docs | <https://deepwiki.com/blockworks-foundation/mango-v4/2.1-mangoclient> | Used to document SDK-driven group/account/transaction methods and why order writes are not REST request specs. |
| Python client package | <https://pypi.org/project/mango-explorer-v4/> | Confirms SDK usage requires a Solana wallet and Mango account for order placement. |
| Solana JavaScript SDK | <https://solana.com/docs/clients/official/javascript> | Used only for the generic Solana wallet/RPC signing boundary; no wallet signing is implemented here. |

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual futures | `Perpetual` | Fixture parser for sanitized group/perp market metadata only. Runtime scan remains `Unsupported`. |
| Cross-margin/lending | n/a | Audited as protocol context; not mapped to exchange gateway balances or risk. |
| Spot/OpenBook | n/a | 项目未实现 Spot/OpenBook。当前 adapter 不接 spot，不能写成交易所不支持。 |

Default Solana RPC URL: `https://api.mainnet-beta.solana.com`

Default Solana WS URL: `wss://api.mainnet-beta.solana.com`

Mango v4 program id recorded by upstream README:
`4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg`

Primary mainnet Mango group recorded by upstream README:
`78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX`

## Authentication And Signing

There is no gateway API key/secret for Mango Markets in this adapter. Private
actions are Solana transactions signed by a user wallet and depend on
MangoAccount ownership, address lookup tables, health/risk checks, priority
fees, transaction confirmation and event queue reconciliation.

The signing fixture records an instruction fingerprint and boundary statement.
It contains no private key, seed phrase, wallet signature, token account,
phone, email or real user account id.

## Endpoint Mapping

| Standard capability | Mango/Solana surface | Current implementation |
| --- | --- | --- |
| Symbol rules | Solana RPC `getAccountInfo` / SDK group scan | Request-spec and parser fixtures only; runtime returns `Unsupported("mango_markets.symbol_rules_require_sdk_or_rpc_scan")`. |
| Order book snapshot | On-chain book side accounts plus event queue decoding | `Unsupported("mango_markets.order_book_requires_onchain_book_and_event_queue_audit")`. |
| Balances | MangoAccount health and token positions | `Unsupported("mango_markets.balances_require_mango_account_health_audit")`. |
| Positions | MangoAccount perp positions | `Unsupported("mango_markets.positions_require_mango_account_perp_audit")`. |
| Fees | Group/risk parameters and market state | `Unsupported("mango_markets.fees_require_group_risk_parameter_audit")`. |
| Place/cancel/amend | Solana wallet-signed transactions | Explicit `Unsupported`; no transaction construction. |
| Batch/cancel-all | Atomic transaction semantics and open order scans | Explicit `Unsupported`. |
| Query/open orders/fills | Event queues/indexers/account scans | Explicit `Unsupported`. |
| Public WS | Solana `accountSubscribe` payload shape | Payload fixture only; no runtime subscription or resync. |
| Private WS | Wallet-owned account subscriptions | Unsupported. |

## Capability Contract

Default `capabilities()` returns:

- `market_types = [Perpetual]`
- `supports_public_rest = false`
- `supports_symbol_rules = false`
- `supports_order_book_snapshot = false`
- `supports_private_rest = false`
- `supports_public_streams = false`
- `supports_private_streams = false`
- all order lifecycle, batch, cancel-all, fee, balance, position, fill, amend,
  quote-market and order-list flags set to `false`

This is a G0 audit adapter, not a trading adapter.

## Unsupported Boundary

The adapter must not enable these without a separate validation task:

- wallet private key, seed phrase or delegated signer handling
- live Solana RPC group/account decoding
- MangoAccount health, margin, collateral, borrow or liquidation calculations
- OpenBook/perp book side and event queue decoding
- public WS sequence/resync and REST/RPC reconciliation
- private account streams, auth renewal or account ownership checks
- place, cancel, amend, cancel-all or batch transaction construction
- live dry-run or real transaction submission

## Fixtures

Fixture path: `tests/fixtures/exchanges/mango_markets/`

- `group_snapshot.json` contains sanitized Mango group/perp market metadata.
- `missing_required_fields.json` verifies parser failure on malformed data.
- `unsupported_boundary.json` records scan-only/trading-disabled status.
- `request_specs/group_account_info.json` validates Solana RPC
  `getAccountInfo` request shape.
- `request_specs/place_order_unsupported.json` records the private write
  boundary.
- `signing_vectors/solana_transaction_boundary.json` records the wallet
  transaction boundary without secrets.
- `ws/account_subscribe.json` and `ws/account_unsubscribe.json` cover Solana
  WS payload shapes only.

## Validation

Allowed commands for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/mango_markets/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway mango_markets --lib --message-format short
```

If app env wiring is changed, also run:

```bash
cargo test -p rustcta-gateway mango_markets --message-format short
```

Do not run `cargo build`, release builds, live `cargo run`, live Solana RPC
pollers, wallet signing, live dry-run transactions or production WebSocket
sessions for this adapter.
