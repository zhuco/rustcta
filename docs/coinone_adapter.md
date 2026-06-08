# Coinone Adapter

Coinone is implemented as a Korea regional spot adapter scoped to KRW markets. The adapter normalizes exchange symbols as `BASE-KRW` and rejects non-KRW quotes before request construction.

## Supported Surfaces

- Public REST: KRW market metadata and order book snapshots.
- Public WebSocket: ticker, trades, and order book channels.
- Private REST: balances, trade fees, place order, quote market buy, cancel, cancel all, query order, open orders, and recent fills.
- Private WebSocket: order/fill and balance/account subscriptions with Coinone payload signature headers.

## Authentication Boundary

Private REST/WS requires `COINONE_SPOT_ACCESS_TOKEN` and `COINONE_SPOT_SECRET_KEY` (or the non-spot-prefixed fallback names). Requests are signed by JSON-serializing the private payload, base64-encoding it into `X-COINONE-PAYLOAD`, then HMAC-SHA512 signing that encoded payload into `X-COINONE-SIGNATURE`.

Only read-only and trade credential scopes are modeled. Transfer and withdraw scopes are intentionally not requested or surfaced.

## Unsupported Boundary

Payment, wallet withdrawal/deposit, deposit address, and fiat-transfer behavior is explicitly unsupported. The adapter should return `Unsupported` for those boundaries and should not route those endpoints through generic private REST helpers.

## Fixtures

Coinone fixtures live under `tests/fixtures/exchanges/coinone/`:

- `request_specs/` documents private request paths and signed-header expectations.
- `signing_vectors/` validates payload base64 and HMAC-SHA512 signatures.
- `parser/markets_krw.json`, `orderbook.json`, `fill.json`, and `balance.json` cover public and private parser surfaces.
- `unsupported_boundary.json` documents the payment/wallet/fiat-transfer boundary.
