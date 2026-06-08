# Independent Reserve gateway adapter

Adapter id: `independentreserve`

## Scope

- Spot markets only.
- AUD, SGD, and USD quote markets are normalized as canonical symbols such as `BTC/AUD`, `BTC/SGD`, and `BTC/USD`.
- Public REST surfaces: market discovery fallback and order book snapshots.
- Private REST surfaces: accounts/balances, place order, cancel order, query order, open orders, and recent fills.
- WebSocket support is exposed as request-spec/session metadata for public order book/trade and private order/trade/account channels.

## Authentication

Independent Reserve private endpoints are POST JSON calls under `/Private/...`. The adapter builds a nonce for each private request, signs the full method URL plus `apiKey`, `nonce`, and endpoint parameters in sorted-key order, and includes the signature in the request body.

This intentionally differs from timestamp-header exchanges. The endpoint version boundary is the public/private path namespace rather than a single `/v3` prefix, so the adapter keeps each Independent Reserve method path explicit.

## Fiat and accounting boundary

AUD, SGD, and USD are handled as spot quote assets and read-only account balance assets. The adapter does not implement:

- Fiat deposits or withdrawals.
- Bank payment rails, PayID/FAST/SWIFT metadata, or address generation.
- Tax reports, statement export, or realized tax-lot accounting.
- Account transfers outside the exchange API order/fill/balance read model.

Any future fiat-ledger expansion must be read-only by default and must not reuse trading credentials for payment operations.
