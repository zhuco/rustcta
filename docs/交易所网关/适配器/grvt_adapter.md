# GRVT Gateway Adapter

Status date: 2026-06-08

Adapter id: `grvt`

Implementation status: conservative Task 9 gateway registration with G0/G1
audit artifacts. GRVT public market-data and trading documentation is mature
enough to map endpoints, but this adapter does not open live REST or
authenticated WebSocket runtime until request-spec, parser, session-cookie and
EIP-712 signer coverage is promoted.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Declared product scope; public REST/WS endpoints are mapped as spec/parser-only. |
| Options | `Option` | Declared product scope for audit; option chain/greeks/order semantics are adapter-specific and not mapped into the shared trading trait. |
| Spot | n/a | 交易所不支持现货（当前 adapter/官方主线口径；如官方 spot 交易面上线需重核）。 |

Default mainnet URLs:

- Market data REST: `https://market-data.grvt.io/full`
- Trading REST: `https://trades.grvt.io/full`
- Auth: `https://edge.grvt.io/auth`
- Public WS: `wss://market-data.grvt.io/ws/full`
- Private WS: `wss://trades.grvt.io/ws/full`

Testnet URLs:

- Market data REST: `https://market-data.testnet.grvt.io/full`
- Trading REST: `https://trades.testnet.grvt.io/full`
- Auth: `https://edge.testnet.grvt.io/auth`

## Authentication

GRVT private REST/WS is not simple per-request HMAC. API-key login returns a
session cookie and `X-Grvt-Account-Id`; write flows additionally require
transaction/order signing with an EIP-712 or external signer boundary. The
gateway config can hold placeholder `api_key`, `session_cookie`, and
`account_id` fields, but private REST remains disabled by default.

No wallet private key, real session cookie, account id, sub-account id, API key,
or order id is committed in fixtures.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/grvt/endpoint_mapping.yaml`.

Current runtime behavior:

- `symbol_rules`: `Unsupported("grvt.symbol_rules_session_spec_only")`
- `order_book`: `Unsupported("grvt.order_book_session_spec_only")`
- `positions`: `Unsupported("grvt.positions_session_spec_only")`
- `place_order`: `Unsupported("grvt.place_order_session_spec_only")`
- `bulk_orders`: `Unsupported("grvt.bulk_orders_session_spec_only")`
- public WS subscribe helper: JSON-RPC payload only
- private WS: `Unsupported("grvt.private_stream_session_spec_only")`

GRVT public order-book WS sequence rule is recorded in fixtures: snapshots use
sequence `0`; deltas are expected to increase by one, and gaps require
reconnect/resubscribe. This task does not claim production low-latency runtime.

## Unsupported Boundary

The following are explicitly not enabled:

- 现货交易：交易所不支持现货（当前 adapter/官方主线口径；如官方 spot 交易面上线需重核）。
- Options chain/greeks/trading through shared Spot/Perp fields.
- Private write REST and bulk orders until EIP-712/external signer fixtures
  prove request construction.
- Transfers, withdrawals, vault, referral and builder APIs.
- Private WebSocket runtime until session-cookie renewal and reconciliation
  fallbacks are implemented.
- Cancel-on-disconnect/dead-man switch until runtime semantics are verified.

## Fixtures

- `tests/fixtures/exchanges/grvt/request_specs/public_book.json`
- `tests/fixtures/exchanges/grvt/request_specs/positions_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/request_specs/open_orders_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/request_specs/query_order_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/request_specs/recent_fills_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/request_specs/create_order_unsupported.json`
- `tests/fixtures/exchanges/grvt/request_specs/cancel_order_unsupported.json`
- `tests/fixtures/exchanges/grvt/request_specs/cancel_all_orders_unsupported.json`
- `tests/fixtures/exchanges/grvt/request_specs/amend_order_unsupported.json`
- `tests/fixtures/exchanges/grvt/request_specs/bulk_orders_unsupported.json`
- `tests/fixtures/exchanges/grvt/signing_vectors/session_cookie_boundary.json`
- `tests/fixtures/exchanges/grvt/ws/book_delta.json`
- `tests/fixtures/exchanges/grvt/ws/private_stream_boundary.json`
- `tests/fixtures/exchanges/grvt/instruments.json`
- `tests/fixtures/exchanges/grvt/orderbook.json`
- unsupported/empty/error/missing-field boundary fixtures

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/grvt/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway grvt --lib --message-format short
```
