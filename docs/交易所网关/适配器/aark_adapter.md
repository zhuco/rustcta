# Aark Gateway Adapter

Status date: 2026-06-08

Adapter id: `aark`

Task: C-40 / AI-D40 from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

Implementation status: conservative Aark profile over the Orderly EVM
API. The adapter implements public market metadata parsing from
`GET /v1/public/info`. Signed REST reads, private account state, order writes,
batch operations and private WebSocket runtime remain explicit `Unsupported`
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
| Spot | n/a | Unsupported. Aark task scope is DeFi/perpetual oriented. |
| Testnet | `Perpetual` | Endpoint mapping records Orderly testnet base URL; not enabled for live or dry-run. |

Default REST base URL: `https://api-evm.orderly.org`

Default testnet REST base URL: `https://testnet-api-evm.orderly.org`

Default public WS URL: `wss://ws-evm.orderly.org/ws/stream`

## Authentication

The disabled config example names sanitized Orderly-style fields:

- `RUSTCTA_AARK_ORDERLY_ACCOUNT_ID`
- `RUSTCTA_AARK_ORDERLY_KEY`
- `RUSTCTA_AARK_ORDERLY_SECRET`

Private REST remains disabled even when those values are present. This is
intentional: account-scoped Ed25519 key handling, permission scope, regional
eligibility and live read-only reconciliation have not been audited.

The signing fixture records the canonical payload shape for a signed Orderly
read request. It does not contain a real private key, account id or signature.

## Endpoint Mapping

| Standard capability | Aark / Orderly endpoint | Current implementation |
| --- | --- | --- |
| Symbol rules | `GET /v1/public/info` | Native public REST parser. |
| Order book snapshot | `GET /v1/orderbook/{symbol}` | Signed read request-spec fixture only; runtime returns `Unsupported("aark.order_book_requires_orderly_account_signed_request")`. |
| Balances | Account signed REST | `Unsupported("aark.balances_require_orderly_account_audit")`. |
| Positions | Account signed REST | `Unsupported("aark.positions_require_orderly_account_audit")`. |
| Fee rate | Account/product specific | `Unsupported("aark.fees_not_mapped_to_gateway_account")`. |
| Place order | Account signed REST | `Unsupported("aark.place_order_requires_orderly_ed25519_account_audit")`. |
| Cancel order | Account signed REST | `Unsupported("aark.cancel_order_requires_orderly_ed25519_account_audit")`. |
| Batch place/cancel | Orderly semantics not audited for Aark profile | Explicit `Unsupported`. |
| Cancel all | Orderly semantics not audited for Aark profile | Explicit `Unsupported`. |
| Query/open orders/fills | Account signed REST | Explicit `Unsupported` until read-only reconciliation is verified. |
| Public WebSocket | Orderly topic payloads | Payload fixtures only; no runtime connection or resync. |
| Private WebSocket | Account auth required | Unsupported. |

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
- `supports_private_rest = false`
- `supports_public_streams = false`
- `supports_private_streams = false`
- all order lifecycle, batch, cancel-all, fee, balance, position, fill, amend,
  quote-market and order-list flags set to `false`

This is a scan-only public-info adapter, not a trading adapter.

## Unsupported Boundary

The adapter must not enable the following without a separate validation task:

- private REST signing with real Orderly account credentials
- account balances, positions, open orders or fills
- order placement, cancellation, cancel-all, amend or batch operations
- Aark/Orderly regional restrictions and KYC eligibility checks
- public WebSocket runtime, sequence gap detection and signed REST resync
- private WebSocket auth, auth renewal and reconciliation fallback

## Fixtures

Fixture path: `tests/fixtures/exchanges/aark/`

- `public_info.json` parses public symbol rules.
- `orderbook_snapshot.json` validates the offline parser for signed read data.
- `request_specs/public_info.json` validates the public REST request shape.
- `request_specs/orderbook_signed_read.json` validates required signed-read headers without real secrets.
- `request_specs/place_order_unsupported.json` records the private write boundary.
- `signing_vectors/orderly_ed25519_boundary.json` records the canonical payload and unsupported signing boundary.
- `ws/public_orderbook_subscribe.json` and `ws/private_auth_payload.json` cover payload shapes only.

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
