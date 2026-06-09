# Aark Gateway Adapter

Status date: 2026-06-08

Adapter id: `aark`

Task: C-40 / AI-D40 from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

Implementation status: conservative Aark profile over the Orderly EVM
API. The adapter implements public market metadata parsing from
`GET /v1/public/info` plus guarded signed REST readbacks for query order, open
orders and recent fills. Private account state, order writes, batch operations
and private WebSocket runtime remain explicit `Unsupported`/offline boundaries
until Aark account onboarding, Orderly key handling, permission scopes and
regional restrictions are separately audited.

## Official Source Table

| Topic | Source | Adapter use |
| --- | --- | --- |
| Aark product surface | <https://aark-digital.gitbook.io/aark-digital/about/perpetual-mode> | Confirms Aark Perpetual Mode is an Orderly-powered CLOB perpetual venue. |
| Aark orderbook model | <https://aark-digital.gitbook.io/aark-digital/about/perpetual-mode/orderbook-trading> | Confirms limit/market order UX is provided through Orderly infrastructure. |
| Aark liquidity/RMM notes | <https://aark-digital.gitbook.io/aark-digital/about/perpetual-mode/high-liquidity> | Used to document the liquidity/vault-style profile boundary without claiming direct vault API support. |
| Aark 1000x isolated mode | <https://aark-digital.gitbook.io/aark-digital/about/1000x-mode> | Used only for liquidation and isolated-margin audit notes; not mapped to gateway order writes. |
| Orderly EVM REST public info | <https://orderly.network/docs/build-on-omnichain/evm-api/restful-api/public/get-available-symbols-public-info> | Used for `get_symbol_rules` parser fixtures. |
| Orderly EVM order book snapshot | <https://orderly.network/docs/build-on-omnichain/evm-api/restful-api/private/orderbook-snapshot> | Recorded as signed read request-spec only. Runtime `get_order_book` stays `Unsupported`. |
| Orderly API introduction/auth | <https://orderly.network/docs/build-on-omnichain/evm-api/restful-api/introduction> | Used to document `orderly-account-id`, `orderly-key`, `orderly-signature`, and `orderly-timestamp` boundary. |

Aark appears as a frontend/profile on top of Orderly rather than a
separate CEX API. The adapter must not pretend Aark has independent
private order, vault, oracle or liquidation endpoints until official
Aark-specific API evidence exists.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Public symbol rules from Orderly public info are parsed. |
| Spot | n/a | 交易所不支持现货。Aark 当前 adapter 口径是 DeFi/perpetual profile。 |
| Testnet | `Perpetual` | Endpoint mapping records Orderly testnet base URL; not enabled for live or dry-run. |

Default REST base URL: `https://api-evm.orderly.org`

Default testnet REST base URL: `https://testnet-api-evm.orderly.org`

Default public WS URL: `wss://ws-evm.orderly.org/ws/stream`

## Authentication

The disabled config example names sanitized Orderly-style fields:

- `RUSTCTA_AARK_ORDERLY_ACCOUNT_ID`
- `RUSTCTA_AARK_ORDERLY_KEY`
- `RUSTCTA_AARK_ORDERLY_SECRET`

Private readback REST remains disabled unless `RUSTCTA_AARK_PRIVATE_REST_ENABLED`
is set and the Orderly account id/key/secret are present. Query/open/fills fail
closed without that guard. Account state, fee reads and all write operations
remain disabled even when credentials are present.

The signing fixture records the canonical payload shape for a signed Orderly
read request. It does not contain a real private key, account id or signature.

## Endpoint Mapping

| Standard capability | Aark / Orderly endpoint | Current implementation |
| --- | --- | --- |
| Symbol rules | `GET /v1/public/info` | Native public REST parser. |
| Order book snapshot | `GET /v1/orderbook/{symbol}` | Signed read request-spec fixture only; runtime returns `Unsupported("aark.order_book_requires_orderly_account_signed_request")`. |
| Balances | `GET /v1/client/holding` | Signed read request-spec fixture only; shared `get_balances` runtime remains project-unimplemented pending Aark/Orderly account audit, parser wiring and read-only reconciliation. |
| Positions | `GET /v1/positions` | Signed read request-spec fixture only; shared `get_positions` runtime remains project-unimplemented pending Aark/Orderly account audit and parser wiring. |
| Fee rate | Orderly `/v1/broker/user_info` | Signed read request-spec fixture only; shared `get_fees` runtime remains project-unimplemented pending Aark builder/account scope audit and parser wiring. |
| Place order | Account signed REST | `Unsupported("aark.place_order_requires_orderly_ed25519_account_audit")`. |
| Cancel order | Account signed REST | `Unsupported("aark.cancel_order_requires_orderly_ed25519_account_audit")`. |
| Amend | `PUT /v1/order` | Signed-write request-spec and parser fixtures only; shared `amend_order` runtime remains project-unimplemented pending Aark account onboarding, audited Ed25519 signer, trade permission guard, post-write query/open/fills reconciliation and dry-run guard. |
| Batch place | `POST /v1/batch-order` | Signed-write request-spec and parser fixtures only; `capabilities_v2` exposes a native/runtime-disabled boundary; shared `batch_place_orders` runtime remains project-unimplemented pending account signer, trade permission guard, partial-failure/missing-item reconciliation and dry-run guard. |
| Batch cancel | Orderly semantics not audited for Aark profile | Explicit `Unsupported`. |
| Cancel all | Orderly semantics not audited for Aark profile | Explicit `Unsupported`. |
| Query order | `GET /v1/order/{order_id}` or `/v1/client/order/{client_order_id}` | Guarded native signed readback behind `RUSTCTA_AARK_PRIVATE_REST_ENABLED` plus Orderly account/key/secret. |
| Open orders | `GET /v1/orders` | Guarded native signed readback behind `RUSTCTA_AARK_PRIVATE_REST_ENABLED` plus Orderly account/key/secret. |
| Recent fills | `GET /v1/trades` | Guarded native signed readback behind `RUSTCTA_AARK_PRIVATE_REST_ENABLED` plus Orderly account/key/secret. |
| Public WebSocket | Orderly topic payloads | Payload fixtures only; no runtime connection or resync. |
| Private WebSocket | Account auth required | Unsupported. |

费率项目未实现/未启用：Aark/Orderly `GET /v1/broker/user_info` 用户费率来源已补 `request_specs/get_fees_user_rates.json` 离线 request-spec 边界，但共享 `get_fees` runtime 仍需完成 builder/account scope 审计、Ed25519 签名读、maker/taker parser 和 `FeeRateSnapshot` wiring。

账户/余额已补离线边界：Aark/Orderly `GET /v1/client/holding` signed readback 已固定 `request_specs/get_balances_signed_read.json`，矩阵按 `get_balances=离线` 记录。共享 `get_balances` runtime 仍需完成账户 onboarding、Ed25519 signing、余额/抵押品 parser、credential scope 和 read-only reconciliation。

## C-40 Audit Notes

Task C-40 explicitly calls out liquidation, vault, oracle and order API
coverage. Current evidence supports only this conservative mapping:

- Liquidation: Aark documentation discusses isolated margin and liquidation
  risk behavior, but no stable public liquidation API is exposed for gateway
  parsing. Runtime liquidation feeds remain Unsupported; Orderly public WS
  liquidation topics require a separate stream-resync task.
- Vault/liquidity: Aark documents RMM/shared Orderly liquidity, but no
  vault deposit, withdrawal or LP accounting API is mapped to the exchange
  gateway. Vault operations stay outside this adapter.
- Oracle: Aark public docs mention risk controls, but do not provide a stable
  oracle endpoint with source/latency metadata. Mark/index/oracle feeds remain
  audit-only until an official endpoint is selected.
- Orders: Orderly documents signed order endpoints, but Aark account
  onboarding, key issuance, region checks, and exact frontend/profile
  semantics are not verified. All private order lifecycle operations return
  explicit `Unsupported`.

## Capability Contract

Default `capabilities()` returns:

- `market_types = [Perpetual]`
- `supports_public_rest = true`
- `supports_symbol_rules = true`
- `supports_order_book_snapshot = false`
- `supports_private_rest = true` only when `RUSTCTA_AARK_PRIVATE_REST_ENABLED`
  and Orderly account/key/secret are configured
- `supports_public_streams = false`
- `supports_private_streams = false`
- `supports_query_order`, `supports_open_orders`, and `supports_recent_fills`
  follow the private readback guard
- all write lifecycle, batch, cancel-all, fee, balance, position, amend,
  quote-market and order-list flags remain `false`

This is a read-only public-info plus guarded order-readback adapter, not a
trading adapter.

## Unsupported Boundary

The adapter must not enable the following without a separate validation task:

- private REST signing with real Orderly account credentials
- account balances, positions, fees or write operations
- order placement, cancellation, cancel-all, amend or batch operations; Orderly
  amend and batch-create are official surfaces but remain project-unimplemented
  for this profile until account onboarding, audited Ed25519 signing, trade
  permission scope, post-write reconciliation and dry-run validation are complete
- Aark/Orderly regional restrictions and KYC eligibility checks
- public WebSocket runtime, sequence gap detection and signed REST resync
- private WebSocket auth, auth renewal and reconciliation fallback

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。Aark Perpetual Mode 走 Orderly infrastructure；Orderly signed account endpoints 支持订单生命周期，但 Aark profile 的账户 onboarding、key 权限、region/KYC 和 frontend/profile 语义还没有在本项目审计。

因此这里不能写成 `交易所不支持下单/撤单`。正确写法是：官方支持，项目未实现/未启用。补交易接口前必须先完成 Aark/Orderly account audit、Ed25519 signing、order/cancel/query/open/fills parser、private read-only reconciliation 和 live-dry-run guard。

## Official Advanced Order Detail

Orderly 官方订单管理列出 `PUT /v1/order` edit 和 `POST /v1/batch-order` batch create。因此 Aark 的 shared `amend_order` 与 `batch_place_orders` 不是交易所不支持，而是项目未实现/未启用；当前已补 `request_specs/amend_order_signed_write.json`、`request_specs/batch_place_orders_signed_write.json`、`parser/amend_order_ack.json` 与 `parser/batch_place_orders_ack.json` 离线边界，并在 `capabilities_v2` 暴露 runtime-disabled endpoint/batch 状态。仍不能提升 live runtime：Aark/Orderly account onboarding、Ed25519 signer、trade credential scope、atomicity reconciliation 和 live-dry-run guard 尚未完成。`place_order_list` 与 `batch_cancel_orders` 未验证到可无损映射的 shared 语义，继续保留 explicit unsupported boundary。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。Orderly EVM private REST 支持 `GET /v1/positions`，返回 margin ratio、free collateral、`position_qty`、mark price、liq price、funding、`margin_mode` 等字段。

因此 Aark 仓位接口写 `官方支持，离线边界已记录，runtime 项目未实现/未启用`，不是 `交易所不支持仓位接口`。当前 `request_specs/get_positions_signed_read.json` 固化了 `GET /v1/positions` 的 signed-read header/source boundary；补运行实现前仍必须完成 Aark/Orderly account audit、Ed25519 signing、positions parser、margin mode/position side 映射和 read-only reconciliation。

## Official WebSocket Order Book Detail

Official Orderly public WS supports `{symbol}@orderbookupdate` every 200ms for
this profile, with `ts` and `prevTs` continuity and quantity `0` delete
semantics. No fixed depth selector is documented (`depth: unspecified`), and no
checksum is documented. Current project support remains
payload/spec-only, but the mapping now records the Orderly EVM stream URL with
`{account_id}`, 200ms interval, `prevTs` gap detection, and request/REST order
book snapshot resync. Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

| Channel | Status | Cadence | Sequence/checksum | Rebuild |
| --- | --- | --- | --- | --- |
| `{symbol}@orderbookupdate` | Payload/spec ready; depth unspecified/no fixed depth | 200ms | `prevTs` must match previous `ts`; no checksum documented | Request signed Orderly `/v1/orderbook/{symbol}` snapshot and resubscribe after `prevTs` gap, reconnect, stale stream, parse error or suspected message loss |

## Fixtures

Fixture path: `tests/fixtures/exchanges/aark/`

- `public_info.json` parses public symbol rules.
- `orderbook_snapshot.json` validates the offline parser for signed read data.
- `private_order.json`, `private_orders.json` and `private_trades.json` validate
  guarded Orderly readback parsers.
- `request_specs/public_info.json` validates the public REST request shape.
- `request_specs/orderbook_signed_read.json` validates required signed-read headers without real secrets.
- `request_specs/query_order.json`, `request_specs/get_open_orders.json` and
  `request_specs/get_recent_fills.json` validate guarded signed-readback request
  shapes without real secrets.
- `request_specs/get_fees_user_rates.json` validates the offline Orderly user fee-rate request boundary.
- `request_specs/get_positions_signed_read.json` validates the `GET /v1/positions` signed-read boundary without real secrets.
- `request_specs/place_order_unsupported.json` records the private write boundary.
- `unsupported_boundary.json` now separates advanced-order project gaps from true unsupported operations: `amend_order` and `batch_place_orders` are project-unimplemented with request/parser fixtures, while `place_order_list` and `batch_cancel_orders` remain unsupported.
- `signing_vectors/orderly_ed25519_boundary.json` records the canonical payload and unsupported signing boundary.
- `ws/public_orderbook_subscribe.json` and `ws/public_orderbook_update.json` cover public orderbook payload shape, 200ms policy and `prevTs` continuity checks.
- `ws/private_auth_payload.json` covers private auth payload shape only.

## Validation

Allowed commands for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/aark/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway aark --lib --message-format short
```

If app env wiring is changed, also run:

```bash
cargo test -p rustcta-gateway aark --message-format short
```

Do not run `cargo build`, release builds, live `cargo run`, live private REST
calls or production WebSocket sessions for this adapter.
## P2 Core Trading Boundary (2026-06-09)

P2 core query/open/fills are guarded Orderly EVM signed readback runtime behind
`RUSTCTA_AARK_PRIVATE_REST_ENABLED` plus Orderly account/key/secret. Place,
cancel, cancel-all and order-list remain offline/unsupported write boundaries;
runtime promotion is still blocked on Aark/Orderly account onboarding, trade
permission guard, post-write reconciliation, and dry-run guard.
