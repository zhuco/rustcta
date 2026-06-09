# KCEX Gateway Adapter

Status date: 2026-06-08

Adapter id: `kcex`

The `kcex` adapter is a conservative product-scope shell. KCEX official pages
and support material show Spot and perpetual futures trading, but this audit did
not find a stable official OpenAPI document for REST endpoints, WebSocket
channels, signing, rate limits, request examples, or order-book sequence
semantics. Live runtime surfaces therefore remain disabled.

## Product Lines

| Product | MarketType | Current status |
| --- | --- | --- |
| Spot | `Spot` | Product line observed on official trading/support pages; REST/WS runtime unsupported until OpenAPI is verified. |
| Perpetual | `Perpetual` | Product line observed on official futures/support pages; REST/WS runtime unsupported until OpenAPI is verified. |
| Dated futures/options | n/a | Not mapped into this adapter. |

## Endpoint Mapping

Mapping file:
`crates/rustcta-exchange-gateway/src/adapters/kcex/endpoint_mapping.yaml`

Current runtime status:

- Public REST: unsupported, `kcex.*_unverified_openapi`.
- Public WS: unsupported, `kcex.public_streams_unverified_openapi`.
- Private REST/WS: unsupported, no verified signing/auth spec.
- Account balances: spec/source fixture only. `get_balances` is mapped to
  `source://kcex/account-balance-openapi-unverified` with
  `tests/fixtures/exchanges/kcex/request_specs/get_balances_account_source.json`;
  runtime remains project-unimplemented until a verified account/balance API,
  signing rule, parser and read-only reconciliation are added.
- Trading: unsupported, no verified order lifecycle API.

## Official Evidence

- KCEX official Spot page shows Spot order form, Depth, Market Trades, fees,
  open orders, order history and trade history.
- KCEX official support docs describe Spot trading and perpetual futures trading
  workflows.
- No official developer/OpenAPI documentation was found in the public support
  category or website resources during this pass.

## Promotion Requirements

Before promoting beyond this shell:

- Verify an official KCEX developer/OpenAPI document and record URL/date.
- Add REST symbol-rules/order-book parser fixtures from official examples.
- Add public order-book WS channel, sequence/checksum, heartbeat, reconnect and
  REST snapshot resync rules.
- Add signing vectors before any private read/write runtime.
- Add dry-run/live-trade guard and reconciliation tests before order placement.

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/kcex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway kcex --lib --message-format short
```
