# Gemini Gateway Adapter

Status date: 2026-06-08.

Gemini is implemented as a Spot-only gateway adapter. REST base URL defaults to `https://api.gemini.com`; public market-data WebSocket defaults to `wss://api.gemini.com/v1/marketdata`; private order-events WebSocket defaults to `wss://api.gemini.com/v1/order/events`.

## Implemented Surface

- Public REST: symbol details and order book snapshots.
- Private REST request construction: balances, order lifecycle, cancel session orders, active orders, and trade fills.
- Signing: private REST and private WS auth use JSON payloads containing `request` and `nonce`, base64-encoded into `Gemini-Payload`, then HMAC-SHA384 hex in `Gemini-Signature`.
- WebSocket specs: public market-data URL construction and private order-events header construction.

## Capability Matrix

| Surface | Runtime |
| --- | --- |
| Products | Spot only |
| Public REST | symbol details, order book snapshot |
| Private REST | balances, limit order lifecycle, active orders, recent fills |
| Public WS | market-data URL construction |
| Private WS | order-events auth headers |
| Order types | limit, post-only |
| Market/quote-market | Unsupported |
| Batch | not advertised; composed batch behavior is not exposed as a capability |
| Fees/positions/reduce-only | Unsupported |

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/gemini/endpoint_mapping.yaml` maps:

- `GET /v1/symbols/details`
- `GET /v1/book/{symbol}`
- `POST /v1/balances`
- `POST /v1/order/new`
- `POST /v1/order/cancel`
- `POST /v1/order/cancel/session`
- `POST /v1/order/status`
- `POST /v1/orders`
- `POST /v1/mytrades`

Private REST operations require request specs under `tests/fixtures/exchanges/gemini/request_specs/`.

## Rate Limits

The adapter tags public REST as `rest_ip`, private account reads as `key`, and order writes/cancels as `orders` in the endpoint mapping. Runtime does not implement a separate local throttler; callers should enforce Gemini's current published rate limits outside this adapter.

## Unsupported Boundary

Auction, block trading, travel-rule, withdrawals, deposits, fiat transfers, custody/clearing workflows, staking, and other funding/compliance surfaces are documented Unsupported and are not connected to runtime. Non-Spot products, positions, market orders, quote-market orders, fees, and reduce-only are also Unsupported. The executable boundary fixture is `tests/fixtures/exchanges/gemini/unsupported_boundary.json`.

## Official References

| Topic | URL |
| --- | --- |
| REST market data | `https://docs.gemini.com/rest/market-data` |
| REST orders | `https://docs.gemini.com/rest/orders` |
| REST account | `https://docs.gemini.com/rest/account` |
| WebSocket order events | `https://docs.gemini.com/websocket/order-events` |

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/gemini/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway gemini --lib --message-format short
```

`cargo build` is intentionally not part of Task 14 validation.
