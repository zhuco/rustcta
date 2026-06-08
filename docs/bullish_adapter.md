# Bullish Gateway Adapter

Status date: 2026-06-08

Adapter id: `bullish`

Task 7 status: public REST G1 plus audited private REST request-spec/signing
fixtures. Live private REST trading is intentionally not promoted yet.

## Product Lines

| Product | MarketType | Current status |
| --- | --- | --- |
| Spot | `Spot` | Public symbol-rules/order-book REST implemented. |
| Perpetual | `Perpetual` | Public symbol-rules/order-book REST implemented. |
| Dated future | `Futures` | Public symbol-rules/order-book REST implemented where Bullish exposes the market. |
| Option | n/a | Explicitly unsupported until the shared gateway has option-native models. |

Production REST base URL: `https://api.exchange.bullish.com`

Sandbox/test URL in config: `https://api.simnext.bullish-test.com`

Default WS URLs:

- Public order book: `wss://api.exchange.bullish.com/trading-api/v1/market-data/orderbook`
- Private data: `wss://api.exchange.bullish.com/trading-api/v1/private-data`

## Authentication

Bullish uses bearer JWT authentication for private REST. Trading commands also
require `BX-TIMESTAMP`, `BX-NONCE`, and `BX-SIGNATURE`.

HMAC signing fixture coverage is included for the official command format:

`timestamp + nonce + method + path + compact_json_body`

The adapter does not manage live JWT refresh, authorizer state, nonce bounds, or
optional nonce windows yet. `place_order`, `cancel_order`, and `amend_order`
therefore return explicit `Unsupported` even though official endpoints exist.

## Endpoint Mapping

Mapping file:
`crates/rustcta-exchange-gateway/src/adapters/bullish/endpoint_mapping.yaml`

Implemented for public REST:

- `GET /trading-api/v1/markets`
- `GET /trading-api/v1/markets/{symbol}/orderbook/hybrid`

Covered as private request-spec/signing fixtures:

- `GET /trading-api/v1/accounts/asset`
- `GET /trading-api/v1/derivatives-positions`
- `GET /trading-api/v2/orders`
- `GET /trading-api/v1/trades`
- `POST /trading-api/v2/orders`
- `POST /trading-api/v2/command` for cancel/amend/cancel-all-style commands

Native batch place/cancel is unsupported; no official native batch endpoint was
found in the audited REST surface.

## WebSocket

Bullish WS uses JSON-RPC 2.0 command messages.

- Subscribe: `method = "subscribe"`, params include `topic` and optional
  `symbol`.
- Heartbeat: `method = "keepalivePing"`.
- Private WS requires JWT-cookie authentication and is not promoted to runtime
  routing in this adapter.

## Fixtures

Fixtures live under `tests/fixtures/exchanges/bullish/`:

- `markets.json`
- `orderbook_hybrid.json`
- `request_specs/place_order.json`
- `request_specs/cancel_order.json`
- `request_specs/get_asset_accounts.json`
- `request_specs/get_derivatives_positions.json`
- `request_specs/get_open_orders.json`
- `request_specs/get_recent_trades.json`
- `request_specs/amend_order.json`
- `request_specs/cancel_all_orders.json`
- `request_specs/get_trading_accounts_fees.json`
- `signing_vectors/hmac_create_order.json`
- `ws_public_orderbook.json`
- `unsupported_boundary.json`

## Unsupported Boundary

The current adapter is not live-trade-enabled. It returns explicit
`Unsupported` for private read execution, all private write execution, batch,
order lists, and live private WS.

Before promotion, add parser tests, transport tests with mocked responses, JWT
lifecycle handling, nonce-bound fixtures, and reconciliation tests against
orders/trades/private WS payloads.

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bullish/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bullish --lib --message-format short
```
