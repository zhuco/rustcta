# WOOFi Pro Gateway Adapter

Status: draft, full signed REST profile with websocket runtime still disabled.
Task: A-36.

## Scope

`woofipro` maps WOOFi Pro perpetual futures through the Orderly API boundary. WOOFi Pro is an onchain perpetual futures venue that uses Orderly account identity and Ed25519 request headers; it is not the same API surface as WOO X (`woo`) and does not reuse the WOO X CEX adapter.

Implemented runtime support:

- `get_symbol_rules` for perpetual markets through `GET /v1/public/info`.
- signed `get_order_book` through `GET /v1/orderbook/{symbol}`.
- signed private REST for balances, positions, place order, quote market order, amend, cancel, batch place/cancel, cancel all, query order, open orders and recent fills.
- Market type: `perpetual`.
- WOOFi Pro defaults: `broker_id=woofi_pro`, Arbitrum One `chain_id=42161`.

Spec-only helpers and fixtures:

- Public websocket subscribe payloads for `{symbol}@orderbookupdate` and `woofi_pro${symbol}@ticker`.
- Private websocket auth payload fixture.
- Orderly Ed25519 canonical payload fixture for signed reads/writes.

Unsupported runtime support:

- Public and private websocket runtime loops.
- Order-list OCO/OTO semantics; the gateway request model does not map cleanly to a verified WOOFi Pro/Orderly order-list endpoint.
- Account fee rate lookup for ordinary users; builder/admin fee routes are not exposed as normal exchange account capability.
- WOOFi Swap, Earn, Stake, wallet registration, deposits, withdrawals, social login and WOO X APIs.

## Endpoints

Primary base URL defaults to `https://api.orderly.org`. `https://api-evm.orderly.org` is retained as a legacy EVM fallback in config, and testnet defaults to `https://testnet-api.orderly.org`.

The endpoint mapping is in:

- `crates/rustcta-exchange-gateway/src/adapters/woofipro/endpoint_mapping.yaml`

Fixture coverage is in:

- `tests/fixtures/exchanges/woofipro/`

## Authentication Boundary

Orderly signed requests use:

- `orderly-account-id`
- `orderly-key`
- `orderly-timestamp`
- `orderly-signature`

The adapter signs requests locally with `ed25519-dalek`. The Orderly secret may be supplied as base58 or 32-byte hex, with an optional `ed25519:` prefix. Canonical payload:

```text
timestamp_ms + METHOD + path_with_query + body
```

The app config keeps private REST disabled unless Orderly credentials are provided and the adapter private REST flag is enabled.

## Configuration

The example config is disabled by default:

- `config/woofipro_gateway_example.yml`

Environment override:

- `RUSTCTA_WOOFIPRO_REST_BASE_URL`
- `RUSTCTA_WOOFIPRO_ORDERLY_ACCOUNT_ID`
- `RUSTCTA_WOOFIPRO_ORDERLY_KEY`
- `RUSTCTA_WOOFIPRO_ORDERLY_SECRET`

Generic app credential mapping also accepts `RUSTCTA_WOOFIPRO_API_KEY` and `RUSTCTA_WOOFIPRO_API_SECRET`, with `RUSTCTA_WOOFIPRO_ORDERLY_ACCOUNT_ID` as the account group.

## Validation

For this task, validation was limited to non-compiling checks per request:

- endpoint mapping validation
- JSON/YAML fixture parsing
- `rustfmt --check` on edited Rust files
- static grep for unfinished markers and fallback error classes

No `cargo build`, `cargo check`, or `cargo test` was run for this task.
