# ModeTrade Gateway Adapter

Status date: 2026-06-08

Adapter id: `modetrade`

Task: A-26 / AI-R26 from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

Implementation status: conservative Mode Trade profile over the Orderly EVM
API. The adapter implements public market metadata parsing from
`GET /v1/public/info`. Signed REST reads, private account state, order writes,
batch operations and private WebSocket runtime remain explicit `Unsupported`
until ModeTrade account onboarding, Orderly key handling, permission scopes and
regional restrictions are separately audited.

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

Private REST remains disabled even when those values are present. This is
intentional: account-scoped Ed25519 key handling, permission scope, regional
eligibility and live read-only reconciliation have not been audited.

The signing fixture records the canonical payload shape for a signed Orderly
read request. It does not contain a real private key, account id or signature.

## Endpoint Mapping

| Standard capability | ModeTrade / Orderly endpoint | Current implementation |
| --- | --- | --- |
| Symbol rules | `GET /v1/public/info` | Native public REST parser. |
| Order book snapshot | `GET /v1/orderbook/{symbol}` | Signed read request-spec fixture only; runtime returns `Unsupported("modetrade.order_book_requires_orderly_account_signed_request")`. |
| Balances | Account signed REST | `Unsupported("modetrade.balances_require_orderly_account_audit")`. |
| Positions | Account signed REST | `Unsupported("modetrade.positions_require_orderly_account_audit")`. |
| Fee rate | Account/product specific | `Unsupported("modetrade.fees_not_mapped_to_gateway_account")`. |
| Place order | Account signed REST | `Unsupported("modetrade.place_order_requires_orderly_ed25519_account_audit")`. |
| Cancel order | Account signed REST | `Unsupported("modetrade.cancel_order_requires_orderly_ed25519_account_audit")`. |
| Batch place/cancel | Orderly semantics not audited for ModeTrade profile | Explicit `Unsupported`. |
| Cancel all | Orderly semantics not audited for ModeTrade profile | Explicit `Unsupported`. |
| Query/open orders/fills | Account signed REST | Explicit `Unsupported` until read-only reconciliation is verified. |
| Public WebSocket | Orderly topic payloads | Payload fixtures only; no runtime connection or resync. |
| Private WebSocket | Account auth required | Unsupported. |

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
- ModeTrade/Orderly regional restrictions and KYC eligibility checks
- public WebSocket runtime, sequence gap detection and signed REST resync
- private WebSocket auth, auth renewal and reconciliation fallback

## Official WebSocket Order Book Detail

Official Orderly public WS supports `{symbol}@orderbookupdate` every 200ms for
this profile, with `ts` and `prevTs` continuity and quantity `0` delete
semantics. No checksum is documented. Current project support is payload/spec
only, so the next mapping task must add the Orderly EVM stream URL with
`{account_id}`, 200ms interval, `prevTs` gap detection, and request/REST order
book snapshot resync. Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

## Fixtures

Fixture path: `tests/fixtures/exchanges/modetrade/`

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
