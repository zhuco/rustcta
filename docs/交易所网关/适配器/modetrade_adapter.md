# ModeTrade Gateway Adapter

Status date: 2026-06-08

Adapter id: `modetrade`

Task: A-26 / AI-R26 from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

Implementation status: conservative Mode Trade profile over the Orderly EVM
API. The adapter implements public market metadata parsing from
`GET /v1/public/info` and guarded signed read-only runtime for query order,
open orders and recent fills. Private account state, fees, order writes, batch
operations and private WebSocket runtime remain explicit offline or
`Unsupported` boundaries until ModeTrade account onboarding, permission scopes,
write reconciliation and regional restrictions are separately audited.

## Official Source Table

| Topic | Source | Adapter use |
| --- | --- | --- |
| Mode Trade product surface | <https://www.mode.trade/> | Confirms Mode Trade is a perpetual DEX style venue, not a generic spot exchange. |
| Orderly EVM REST public info | <https://orderly.network/docs/build-on-omnichain/evm-api/restful-api/public/get-available-symbols-public-info> | Used for `get_symbol_rules` parser fixtures. |
| Orderly EVM order book snapshot | <https://orderly.network/docs/build-on-omnichain/evm-api/restful-api/private/orderbook-snapshot> | Recorded as signed read request-spec only. Runtime `get_order_book` stays `Unsupported`. |
| Orderly API introduction/auth | <https://orderly.network/docs/build-on-omnichain/evm-api/restful-api/introduction> | Used to document `orderly-account-id`, `orderly-key`, `orderly-signature`, and `orderly-timestamp` boundary. |

Mode Trade appears as a frontend/profile on top of Orderly rather than a
separate CEX API. The adapter must not pretend ModeTrade has independent
private order semantics until official ModeTrade-specific trading API evidence
exists.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Public symbol rules from Orderly public info are parsed. |
| Spot | n/a | 交易所不支持现货。ModeTrade 当前 adapter 口径是 DeFi/perpetual profile。 |
| Testnet | `Perpetual` | Endpoint mapping records Orderly testnet base URL; not enabled for live or dry-run. |

Default REST base URL: `https://api-evm.orderly.org`

Default testnet REST base URL: `https://testnet-api-evm.orderly.org`

Default public WS URL: `wss://ws-evm.orderly.org/ws/stream`

## Authentication

The disabled config example names sanitized Orderly-style fields:

- `RUSTCTA_MODETRADE_ORDERLY_ACCOUNT_ID`
- `RUSTCTA_MODETRADE_ORDERLY_KEY`
- `RUSTCTA_MODETRADE_ORDERLY_SECRET`

Private REST fails closed unless `RUSTCTA_MODETRADE_PRIVATE_REST_ENABLED=true`
and all three Orderly credential fields are present. With that guard enabled,
only `query_order`, `get_open_orders` and `get_recent_fills` use signed
read-only REST. Balances, positions, fees and writes remain disabled.

The signing fixture records the canonical payload shape for a signed Orderly
read request. It does not contain a real private key, account id or signature.

## Endpoint Mapping

| Standard capability | ModeTrade / Orderly endpoint | Current implementation |
| --- | --- | --- |
| Symbol rules | `GET /v1/public/info` | Native public REST parser. |
| Order book snapshot | `GET /v1/orderbook/{symbol}` | Signed read request-spec fixture only; runtime returns `Unsupported("modetrade.order_book_requires_orderly_account_signed_request")`. |
| Balances | `GET /v1/client/holding` | Signed read request-spec fixture only; shared `get_balances` runtime remains project-unimplemented pending ModeTrade/Orderly account audit, parser wiring and read-only reconciliation. |
| Positions | `GET /v1/positions` | Signed read request-spec fixture only; shared `get_positions` runtime remains project-unimplemented pending ModeTrade/Orderly account audit and parser wiring. |
| Fee rate | Orderly `/v1/broker/user_info` | Signed read request-spec fixture only; shared `get_fees` runtime remains project-unimplemented pending ModeTrade builder/account scope audit and parser wiring. |
| Place order | Account signed REST | `Unsupported("modetrade.place_order_requires_orderly_ed25519_account_audit")`. |
| Cancel order | Account signed REST | `Unsupported("modetrade.cancel_order_requires_orderly_ed25519_account_audit")`. |
| Amend | `PUT /v1/order` | Signed-write request-spec plus ack parser fixture only; shared `amend_order` runtime returns `Unsupported("modetrade.amend_order_spec_only_no_orderly_ed25519_permission_reconciliation_guard")` pending ModeTrade account onboarding, audited Ed25519 signer, trade permission guard, post-write query/open/fills reconciliation and dry-run guard. |
| Batch place | `POST /v1/batch-order` | Signed-write request-spec plus ack parser fixtures only; `capabilities_v2.batch_place_orders` records native/partial spec-only boundary, while shared runtime returns `Unsupported("modetrade.batch_place_orders_spec_only_no_orderly_ed25519_permission_reconciliation_guard")` pending account signer, permission guard, partial-failure/missing-item/post-write reconciliation and dry-run guard. |
| Batch cancel | Orderly semantics not audited for ModeTrade profile | Explicit `Unsupported`. |
| Cancel all | Orderly semantics not audited for ModeTrade profile | Explicit `Unsupported`. |
| Query order | `GET /v1/order/{order_id}` or `GET /v1/client/order/{client_order_id}` | Guarded signed read-only runtime. |
| Open orders | `GET /v1/orders` | Guarded signed read-only runtime with `status=INCOMPLETE`. |
| Recent fills | `GET /v1/trades` | Guarded signed read-only runtime. |
| Public WebSocket | Orderly topic payloads | Payload fixtures only; no runtime connection or resync. |
| Private WebSocket | Account auth required | Unsupported. |

## Capability Contract

Default `capabilities()` returns:

- `market_types = [Perpetual]`
- `supports_public_rest = true`
- `supports_symbol_rules = true`
- `supports_order_book_snapshot = false`
- `supports_private_rest = true` only when `RUSTCTA_MODETRADE_PRIVATE_REST_ENABLED` plus Orderly credentials are configured
- `supports_public_streams = false`
- `supports_private_streams = false`
- `supports_query_order`, `supports_open_orders` and `supports_recent_fills`
  follow the guarded private REST flag
- writes, batch, cancel-all, fee, balance, position, amend, quote-market and
  order-list flags remain `false`

This is a read-only guarded adapter for order readbacks, not a trading adapter.

## Unsupported Boundary

The adapter must not enable the following without a separate validation task:

- account balances or positions
- order placement, cancellation, cancel-all, amend or batch operations; Orderly
  amend and batch-create are official surfaces but remain project-unimplemented
  for this profile until account onboarding, audited Ed25519 signing, trade
  permission scope, post-write reconciliation and dry-run validation are complete
- ModeTrade/Orderly regional restrictions and KYC eligibility checks
- public WebSocket runtime, sequence gap detection and signed REST resync
- private WebSocket auth, auth renewal and reconciliation fallback

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。Mode Trade 是 Orderly EVM profile；Orderly signed account endpoints 支持订单生命周期，但 ModeTrade profile 的账户 onboarding、key 权限、region/KYC 和 profile 语义未在本项目审计。

因此这里不能写成 `交易所不支持下单/撤单`。正确写法是：官方支持，项目未实现/未启用。补写接口前必须先完成 ModeTrade/Orderly account audit、trade permission guard、write 后 query/open/fills reconciliation 和 live-dry-run guard。

## Official Advanced Order Detail

Orderly 官方订单管理列出 `PUT /v1/order` edit 和 `POST /v1/batch-order` batch create。因此 ModeTrade 的 shared `amend_order` 与 `batch_place_orders` 不是交易所不支持，而是项目未实现/未启用；当前已补 `request_specs/amend_order_signed_write.json`、`request_specs/batch_place_orders_signed_write.json`、`parser/amend_order_ack.json`、`parser/batch_place_orders_ack.json`、`parser/batch_place_orders_partial_ack.json` 与 `parser/batch_place_orders_missing_item_ack.json` 离线边界，并在 `capabilities_v2` 暴露 runtime-disabled endpoint/batch metadata。batch parser fixture 可生成 partial failure 和 missing item reconciliation report，但仍不能提升 live runtime：项目尚未接 ModeTrade/Orderly account onboarding、audited Ed25519 signing runtime、trade permission scope guard、write 后 query/open/fills reconciliation、partial failure/missing item reconciliation 和 live-dry-run guard。`place_order_list` 与 `batch_cancel_orders` 未验证到可无损映射的 shared 语义，继续保留 explicit unsupported boundary。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。ModeTrade 是 Orderly EVM profile，同源 Orderly `GET /v1/positions` 可作为账户仓位来源。

因此 ModeTrade 仓位接口写 `官方支持，离线边界已记录，runtime 项目未实现/未启用`。当前 `request_specs/get_positions_signed_read.json` 固化了 `GET /v1/positions` 的 signed-read header/source boundary；补运行实现前仍必须完成 ModeTrade/Orderly account audit、Ed25519 signing、positions parser、margin mode/position side 映射和 read-only reconciliation。

## Official WebSocket Order Book Detail

Official Orderly public WS supports `{symbol}@orderbookupdate` every 200ms for
this profile, with `ts` and `prevTs` continuity and quantity `0` delete
semantics. No fixed depth selector is documented (`depth: unspecified`), and no
checksum is documented. Current project support is payload/spec only; the
mapping records the Orderly EVM stream URL with `{account_id}`, 200ms interval,
`prevTs` gap detection, and request/REST order book snapshot resync. Source
batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

| Channel | Status | Cadence | Sequence/checksum | Rebuild |
| --- | --- | --- | --- | --- |
| `{symbol}@orderbookupdate` | Payload/spec ready; depth unspecified/no fixed depth | 200ms | `prevTs` must match previous `ts`; no checksum documented | Request signed Orderly `/v1/orderbook/{symbol}` snapshot and resubscribe after `prevTs` gap, reconnect, stale stream, parse error or suspected message loss |

## Fixtures

Fixture path: `tests/fixtures/exchanges/modetrade/`

- `public_info.json` parses public symbol rules.
- `orderbook_snapshot.json` validates the offline parser for signed read data.
- `order_success.json`, `open_orders_success.json` and
  `recent_fills_success.json` validate guarded private readback parsers.
- `request_specs/public_info.json` validates the public REST request shape.
- `request_specs/orderbook_signed_read.json` validates required signed-read headers without real secrets.
- `request_specs/query_order_signed_read.json`,
  `request_specs/get_open_orders_signed_read.json` and
  `request_specs/get_recent_fills_signed_read.json` validate signed readback
  request shapes without real secrets.
- `request_specs/get_fees_user_rates.json` validates the offline Orderly user fee-rate request boundary.
- `request_specs/get_positions_signed_read.json` validates the `GET /v1/positions` signed-read boundary without real secrets.
- `request_specs/amend_order_signed_write.json` and `request_specs/batch_place_orders_signed_write.json` validate Orderly signed-write request boundaries without real secrets.
- `parser/batch_place_orders_partial_ack.json` and `parser/batch_place_orders_missing_item_ack.json` pin partial failure and missing item reconciliation reports for the spec-only batch-create boundary.
- `parser/amend_order_ack.json` and `parser/batch_place_orders_ack.json` validate offline advanced-order ack parsing only.
- `request_specs/place_order_unsupported.json` records the private write boundary.
- `signing_vectors/orderly_ed25519_boundary.json` records the canonical payload and unsupported signing boundary.
- `ws/public_orderbook_subscribe.json` and `ws/private_auth_payload.json` cover payload shapes only.

## Validation

Allowed commands for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/modetrade/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway modetrade --lib --message-format short
```

If app env wiring is changed, also run:

```bash
cargo test -p rustcta-gateway modetrade --message-format short
```

Do not run `cargo build`, release builds, live `cargo run`, live private REST
calls or production WebSocket sessions for this adapter.

## Fee Boundary

费率来源在 Orderly/ModeTrade account 资料中存在；当前已补 `request_specs/get_fees_user_rates.json` 离线 request-spec 边界，对应 Orderly `GET /v1/broker/user_info` 用户费率来源。shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 builder/account scope 审计、Ed25519 signing、maker/taker parser 和 `FeeRateSnapshot` wiring。

账户/余额已补离线边界：ModeTrade/Orderly `GET /v1/client/holding` signed readback 已固定 `request_specs/get_balances_signed_read.json`，矩阵按 `get_balances=离线` 记录。共享 `get_balances` runtime 仍需完成账户 onboarding、Ed25519 signing、余额/抵押品 parser、credential scope 和 REST/WS reconciliation。
## P2 Core Trading Boundary (2026-06-09)

P2 core query/open/fills are now guarded read-only runtime via Orderly signed
REST. Place/cancel remain offline/spec-only write boundaries; cancel-all and
order-list remain unsupported shared semantics.
