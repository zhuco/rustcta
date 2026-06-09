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

Spot/OpenBook 边界写入 `spot_product status: project_unimplemented`：当前只记录 Mango group/perp market 与 MangoAccount source-boundary。补 Spot/OpenBook 前需要 Mango group spot market discovery、OpenBook market/bids/asks/event queue decoding、MangoAccount spot open-orders 扫描、slot/reorg/indexer 对账和 Solana wallet transaction builder。

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

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。Mango v4 SDK 支持 wallet/MangoAccount 驱动的订单交易，需要 Solana wallet-signed transactions、MangoAccount health/order id、slot/reorg 和 event queue/indexer 对账。

因此这里写 `官方协议可交易，项目未实现链上交易 runtime`，不是 `交易所不支持下单/撤单`。当前 adapter 不构造、不签名、不提交任何交易。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。Mango v4 在 MangoAccount 中跟踪 PerpPosition，包含 base lots、quote native、funding settlement、market index 等。

因此仓位读取写 `官方协议支持，离线 source-boundary 已记录，项目未实现链上仓位 runtime`。`endpoint_mapping.yaml` 的 `get_positions` 使用 `solana-rpc://getAccountInfo/mango_account` spec-only 边界和 `tests/fixtures/exchanges/mango_markets/request_specs/get_positions_account_source.json`，只记录 MangoAccount `perp_positions` 的 Solana RPC/account source；本地测试只验证 source-boundary fixture 结构和 runtime guard。补 runtime 前仍必须完成 Solana/MangoAccount ownership/account decoding、PerpPosition parser、health/PnL/funding fields、slot/reorg/indexer reconciliation。

账户/余额接口写 `官方协议支持，项目未实现/未启用`：MangoAccount 可读取 collateral、borrows、health 和 token positions。`endpoint_mapping.yaml` 已将 `get_balances` 写成 `solana-rpc://getAccountInfo/mango_account` spec-only source boundary，并绑定 `tests/fixtures/exchanges/mango_markets/request_specs/get_balances_account_source.json`，矩阵应为 `get_balances=离线`；shared runtime 尚未接 Solana account decoding、health parser 与 indexer reconciliation。

## Endpoint Mapping

| Standard capability | Mango/Solana surface | Current implementation |
| --- | --- | --- |
| Symbol rules | Solana RPC `getAccountInfo` / SDK group scan | Request-spec and parser fixtures only; runtime returns `Unsupported("mango_markets.symbol_rules_require_sdk_or_rpc_scan")`. |
| Order book snapshot | On-chain book side accounts plus event queue decoding | `Unsupported("mango_markets.order_book_requires_onchain_book_and_event_queue_audit")`. |
| Balances | MangoAccount health and token positions | Spec/source fixture only; no live MangoAccount decoding or indexer reconciliation is executed. |
| Positions | MangoAccount perp positions | Solana RPC/MangoAccount source fixture only; runtime still returns `Unsupported("mango_markets.positions_require_mango_account_perp_audit")`. Parser coverage validates the source boundary, not live decoded positions. |
| Fees | Group/risk parameters and market state | `Unsupported("mango_markets.fees_require_group_risk_parameter_audit")`. |
| Place/cancel/amend | Solana wallet-signed transactions | Explicit `Unsupported`; no transaction construction. |
| Batch/cancel-all | Atomic transaction semantics and open order scans | Explicit `Unsupported`. |
| Query/open orders/fills | Event queues/indexers/account scans | Explicit `Unsupported`. |
| Public WS | Solana `accountSubscribe` payload shape | YAML records public order book WS as unsupported; `accountSubscribe` is payload evidence only, not a unified orderbook channel. |
| Private WS | Wallet-owned account subscriptions | Unsupported. |

费率项目未实现/未启用：Mango group/market fee params 已记录到 `tests/fixtures/exchanges/mango_markets/request_specs/get_fees_source_boundary.json`，适用 Margin/Perpetual 协议源。该边界只证明 group/market/account source 口径；生产 runtime 仍需 Mango group decoder、market fee scope、MangoAccount context、slot/reorg guard 和 indexer/on-chain reconciliation，不能用静态零费率占位。

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。当前 adapter 只确认 Solana RPC `accountSubscribe`，这不是统一交易所公共订单簿 WS；它只通知链上账户变化，带 slot/context，不提供交易所级 orderbook sequence/checksum。

因此单交易所文档写 `交易所不支持当前公共订单簿 WS runtime`。如后续要接 Mango/OpenBook/Serum 盘口，需要单独链上 indexer、account decoding、slot/reorg/replay 和 REST/RPC reconciliation 设计。
`endpoint_mapping.yaml` correspondingly sets `websocket.public.support: unsupported`
and keeps `accountSubscribe` as Solana RPC metadata only.

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
- `request_specs/get_positions_account_source.json` records the MangoAccount
  `perp_positions` source boundary for `get_positions`.
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

`get_positions` remains `离线`: `parse_position_source_boundary` validates the sanitized source fixture, while the live runtime still returns the MangoAccount boundary because there is no audited Solana account decoder, slot freshness guard, decoded PerpPosition response, or indexer/on-chain reconciliation path.
## P2 Core Trading Boundary (2026-06-09)

P2 core place/cancel/query/open/fills are offline/spec-only Solana/MangoAccount boundaries; cancel-all/order-list remain unsupported shared semantics. Runtime promotion is blocked on wallet transaction construction, MangoAccount health/order id decoding, event queue/indexer parsing, slot/reorg reconciliation, and dry-run guard.

## P2 Product Line Boundary (2026-06-09)

`spot_product` covers the official Mango/OpenBook spot source boundary and remains `project_unimplemented`, not exchange-unsupported. Mango group state can reference Spot/OpenBook markets, while this adapter is scoped to Mango margin/perpetual Solana scans and MangoAccount source boundaries.

Do not promote OpenBook runtime from the margin/perpetual scanner. Promotion requires group spot-market decoding, OpenBook market/bids/asks/event-queue public account parsers, MangoAccount spot open-order and balance ownership checks, spot order lifecycle transaction builders, slot/reorg handling, and indexer reconciliation.
