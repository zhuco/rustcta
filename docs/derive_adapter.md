# Derive Gateway Adapter

Status: Task 8 offline-verifiable adapter for Derive options/perp DeFi scope.

## Scope

- Products: options and perpetuals.
- JSON-RPC base URL: `https://api.derive.xyz`.
- WebSocket URL: `wss://api.derive.xyz/ws`.
- Public scope: instrument metadata, order book/ticker/trade channel payloads.
- Private scope: session/account model, balances/positions/open-orders/order request specs, and offline session-signing fixtures.

## Signing And Account Model

Derive private methods use a wallet/subaccount/session-key model. The adapter keeps the model explicit:

- `wallet`: owner wallet identifier, configured only through env/config.
- `subaccount_id`: target trading subaccount.
- `session_key` and `session_secret`: scoped session credential placeholders.
- Private JSON-RPC methods such as `private/order`, `private/cancel`, `private/get_open_orders`, and `private/get_positions` require session-key authorization.

Fixtures use `fixture-subaccount`, `fixture-session-key`, and deterministic HMAC sentinel material. No real wallet, session token, or chain account fixture is committed.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/derive/endpoint_mapping.yaml`.

Covered operations:

- `symbol_rules`: JSON-RPC `public/get_all_instruments` / instruments metadata.
- `order_book`: public orderbook reference / JSON-RPC route.
- `balances`: private collateral/balance route boundary.
- `positions`: JSON-RPC `private/get_positions`.
- `place_order`: JSON-RPC `private/order`, offline request-spec only.
- `cancel_order`: JSON-RPC `private/cancel`, offline request-spec only.
- `open_orders`: JSON-RPC `private/get_open_orders`.

## Capability Boundary

The runtime adapter advertises public REST and public WS specs. Private reads are gated on a configured subaccount/session key. Private writes and private WebSocket runtime return explicit `Unsupported` until live-dry-run promotion validates session-key signing, permission levels, and REST reconciliation.

Options and perp metadata are adapter-specific in `tests/fixtures/exchanges/derive/instruments.json`; no shared trait was expanded.

WebSocket fixtures cover subscribe, unsubscribe, heartbeat ping/pong, login/auth boundary, and private order event parser samples. Private stream disconnects require JSON-RPC REST reconciliation over balances, positions, open orders, and trade history before live promotion.

Unsupported or deferred:

- Live order placement/cancel.
- Admin-only cancel-all promotion.
- Native batch place/cancel.
- Shared option Greeks/risk model.

## Validation

Allowed targeted commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/derive/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway derive --lib --message-format short
```
