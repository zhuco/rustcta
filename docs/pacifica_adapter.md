# Pacifica Gateway Adapter

Adapter id: `pacifica`

Status: Task 10 initial adapter. Public REST market info and book snapshots are
implemented through official endpoints. Public/private WebSocket streams and
private writes are offline request-spec/signing/parser fixtures until an
agent-wallet live dry-run task verifies permissions, sequencing, and regional
access.

## Official Materials

| Topic | Source |
| --- | --- |
| API overview | <https://docs.pacifica.fi/api-documentation/api> |
| Market info | <https://pacifica.gitbook.io/docs/api-documentation/api/rest-api/markets/get-market-info> |
| Order book | <https://pacifica.gitbook.io/docs/api-documentation/api/rest-api/markets/get-orderbook> |
| Open orders | <https://docs.pacifica.fi/api-documentation/api/rest-api/orders/get-open-orders> |
| Create limit order | <https://docs.pacifica.fi/api-documentation/api/rest-api/orders/create-limit-order> |
| Cancel all orders | <https://docs.pacifica.fi/api-documentation/api/rest-api/orders/cancel-all-orders> |
| Signing implementation | <https://pacifica.gitbook.io/closed-alpha/api-documentation/api/signing/implementation> |
| Operation types | <https://pacifica.gitbook.io/docs/api-documentation/api/signing/operation-types> |
| WebSocket base URLs | <https://pacifica.gitbook.io/docs/api-documentation/api/websocket> |
| Rate limits | <https://docs.pacifica.fi/api-documentation/api/rate-limits> |

## Product Scope

- Declared market type: `MarketType::Perpetual`.
- Default REST URL: `https://api.pacifica.fi`
- Testnet REST URL: `https://test-api.pacifica.fi`
- Mainnet WS URL: `wss://ws.pacifica.fi/ws`
- Testnet WS URL: `wss://test-ws.pacifica.fi/ws`

## Implemented Public REST

- `get_symbol_rules` uses `GET /api/v1/info` and maps tick/lot/order-size
  fields to `SymbolRules`.
- `get_order_book` uses `GET /api/v1/book?symbol=<symbol>` and maps the first
  level array to standard bid/ask snapshots.

## Signing

Pacifica signs POST requests with deterministic compact JSON:

1. Create a header with `timestamp`, `expiry_window`, and operation `type`.
2. Add a `data` object containing the operation fields.
3. Recursively sort keys and serialize compact JSON.
4. Sign UTF-8 bytes using Ed25519.
5. Send the base58 signature in the request body with `account`, optional
   `agent_wallet`, `timestamp`, and `expiry_window`.

The adapter implements deterministic payload construction and Ed25519 signing
for base64, hex, and base58 local test keys. Sanitized fixture:

`tests/fixtures/exchanges/pacifica/signing_vectors/create_order_ed25519.json`

## WebSocket Policy

- Subscription payload: `{"method":"subscribe","params":{"source":"book","symbol":"BTC"}}`
- Heartbeat payload: `{"method":"ping"}`
- Timeout boundary: send a heartbeat before 60 seconds of silence; reconnect
  before the 24-hour server lifetime. Runtime is not wired into the gateway
  supervisor yet.
- Reconciliation fallback: use REST `/api/v1/book` for book snapshots and REST
  `/api/v1/orders` for open-order snapshots; use `last_order_id`/`li` ordering
  where present.

## Capability Boundary

Unsupported or request-spec-only:

- `place_order`, `cancel_order`, `cancel_all_orders`, `batch_place_orders`,
  `amend_order`, and `query_order` are request-spec/signing only.
- Public WebSocket runtime is spec-only; payload and parser fixtures are kept
  for a later resync validation task.
- Private WebSocket account streams are spec-only until signed subscription and
  resync are live verified.
- Leverage, margin mode, TPSL, withdrawals, subaccounts, API key management, and
  builder-code operations are not exposed through the shared trading runtime.
- Dead-man switch is not mapped.

## Endpoint Mapping

Machine-readable mapping:

`crates/rustcta-exchange-gateway/src/adapters/pacifica/endpoint_mapping.yaml`

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/pacifica/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway pacifica --lib --message-format short
```

Do not run `cargo build`, production private streams, or live trading commands
for this adapter.
