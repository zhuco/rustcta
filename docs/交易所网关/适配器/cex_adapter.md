# CEX.IO Gateway Adapter

Status date: 2026-06-08

## Scope

Adapter id: `cex`.

This adapter covers CEX.IO spot public REST at G1:

- `get_symbol_rules` via `GET /currency_limits`
- `get_order_book` via `GET /order_book/{symbol1}/{symbol2}/`
- offline REST and WebSocket signing/payload fixtures
- explicit `Unsupported` boundaries for private runtime trading, fiat ledger operations, margin, futures and transfers

## Official Interfaces

| Surface | URL | Status |
| --- | --- | --- |
| Legacy REST API | `https://cex.io/api` | Used for public REST implementation |
| WebSocket API | `wss://ws.cex.io/ws/` | Payload and heartbeat helpers only |
| Private REST | JSON POST body with `nonce`, `key`, `signature` | Request-spec boundary only |

The REST docs publish `currency_limits` and `order_book` public endpoints. Private REST uses HMAC-SHA256 over `nonce + user_id + api_key`; WebSocket auth signs `timestamp + api_key`.

## Capability

| Capability | Status | Notes |
| --- | --- | --- |
| Spot symbol rules | Native | Parsed from `currency_limits.data.pairs` |
| Spot order book snapshot | Native | `depth` query is bounded to 1-100 locally |
| Private balances/orders/fills | Unsupported | Requires follow-up read-only request-spec promotion |
| Private order writes | Unsupported | Fixture documents request shape; adapter does not send signed writes |
| Batch place/cancel | Unsupported | CEX.IO mass-cancel-place is not mapped to generic batch semantics |
| WebSocket runtime | Unsupported | Payload/auth/pong helpers are present; no production socket runtime |
| Fiat ledger, deposit, withdrawal, transfer | Unsupported | Not a trading runtime operation |

## Files

- Adapter: `crates/rustcta-exchange-gateway/src/adapters/cex/`
- Mapping: `crates/rustcta-exchange-gateway/src/adapters/cex/endpoint_mapping.yaml`
- Fixtures: `tests/fixtures/exchanges/cex/`
- Disabled config: `config/cex_gateway_example.yml`

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway cex --lib --message-format short
cargo test -p rustcta-gateway cex --message-format short
```

Do not run `cargo build` for this task.
