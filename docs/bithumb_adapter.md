# Bithumb Gateway Adapter

Status date: 2026-06-08

## Scope

- Adapter id: `bithumb`
- Product line: Korean Spot only
- Markets: KRW spot markets using `QUOTE-BASE` exchange symbols, for example `KRW-BTC`
- REST base URL: `https://api.bithumb.com`
- Public WS: `wss://ws-api.bithumb.com/websocket/v1`
- Private WS: `wss://ws-api.bithumb.com/websocket/v1/private`

This adapter is offline-valid for public REST, private REST request construction, JWT signing vectors, and WebSocket payload/parser specs. It does not perform live trading validation.

## Official API Notes

- Public market list and order book use `/v1/market/all` and `/v1/orderbook`.
- Private REST uses `Authorization: Bearer <jwt>`.
- JWT payload includes `access_key`, `nonce`, Bithumb `timestamp`, and `query_hash`/`query_hash_alg=SHA512` when query or body parameters are present.
- Private write request specs are fixture-backed under `tests/fixtures/exchanges/bithumb/request_specs/`.

## Rate Limits

The mapping keeps conservative local buckets:

- `bithumb_rest`: 10 requests per second
- `bithumb_orders`: 5 order requests per second
- `bithumb_ws`: conservative connection/message budget

If Bithumb account-level policy or region controls are stricter for a key, runtime config must lower these limits. This adapter does not implement region bypass.

## Implemented Offline Surface

- Symbol rules: Spot market list parser
- Order book: REST snapshot parser and public WS orderbook payload spec
- Private read REST: balances, fee snapshot via order chance, order query, open orders, recent fills
- Private write REST: place order, quote market buy, cancel order request construction
- Private WS: order/fill/balance subscription payloads and REST reconciliation fallback

## Unsupported Boundary

- Positions, leverage, margin mode, position mode: unsupported because Bithumb Spot has no standard derivative position model.
- Amend order: unsupported until a native endpoint is verified against the shared Spot amend contract.
- Batch place/cancel and cancel-all: unsupported; no native atomic batch endpoint is claimed.
- Fiat funding, withdrawal, transfer, and bank operations are not part of the gateway runtime.

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bithumb/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bithumb --lib --message-format short
```
