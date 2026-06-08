# BTC Markets gateway adapter

Adapter id: `btcmarkets`

## Scope

- Spot markets only.
- AUD quote markets are normalized as canonical symbols such as `BTC/AUD` and exchange symbols such as `BTC-AUD`.
- Public REST surfaces: market rules and order book snapshots.
- Private REST surfaces: balances, place order, cancel order, query order, open orders, and recent fills.
- WebSocket support is exposed as request-spec/session metadata for public order book/trade and private order/fund channels.

## Authentication

BTC Markets v3 private REST requests use timestamped HMAC-SHA512 request signing. The adapter signs the uppercase HTTP method, endpoint path, millisecond timestamp, and JSON body into `BM-AUTH-SIGNATURE`, and sends the API key and timestamp in BTC Markets auth headers.

The adapter keeps the v3 endpoint path explicit (`/v3/...`) so older endpoint versions are not mixed into order or accounting calls.

## Fiat and accounting boundary

AUD is handled as a spot quote asset and as a read-only balance asset. The adapter does not implement:

- Fiat deposits or withdrawals.
- Bank payment rails, BPAY/PayID metadata, or address generation.
- Tax reports, EOFY statements, or realized tax-lot accounting.
- Internal transfers outside the exchange API order/fill/balance read model.

Any future fiat-ledger expansion must be read-only by default and must not reuse trading credentials for payment operations.
