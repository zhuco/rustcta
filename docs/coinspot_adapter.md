# CoinSpot Adapter

Scope: Australian AUD spot REST only.

Base URLs:

- Public REST: `https://www.coinspot.com.au/pubapi/v2/*`
- Private REST: `https://www.coinspot.com.au/api/v2/*`
- Read-only private REST: `https://www.coinspot.com.au/api/v2/ro/*`

Implemented:

- Public REST symbol rules from `/pubapi/v2/markets`.
- Public order book snapshots from `/pubapi/v2/orders/open`.
- Private HMAC-SHA512 JSON signing with `nonce`, `key`, and `sign`.
- Private balances, fees, open orders, recent fills, limit buy/sell, market buy/sell, and quote-value market buy request construction.
- Offline request specs, signing vectors, and parser fixtures under `tests/fixtures/exchanges/coinspot/`.

Explicit boundaries:

- Non-AUD markets are rejected as unsupported.
- Streams are unsupported.
- Positions are unsupported because CoinSpot is spot-only.
- Cancel, amend, query-order, and batch operations are unsupported because CoinSpot exposes side-specific order APIs that are not safely mapped from generic gateway requests without side/order-context guarantees.
- Fill history filters and pagination are unsupported.
- Fiat deposit, withdrawal, and payment APIs are documentation boundaries only and are not runtime gateway operations.

Validation:

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinspot/endpoint_mapping.yaml`
- `python3 -m json.tool tests/fixtures/exchanges/coinspot/...`
- `cargo test -p rustcta-exchange-gateway coinspot --lib --message-format short` once unrelated parallel adapters compile.
