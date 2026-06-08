# Coins.ph Gateway Adapter

Status: independent Philippines Spot REST adapter slice for Task 19.

## Scope

- Public REST: PHP spot symbol rules from `GET /openapi/v1/exchangeInfo` and order book snapshots from `GET /openapi/quote/v1/depth`.
- Private REST: balances, trade fees, place order, cancel order, query order, open orders, and recent fills.
- Public/private WebSocket runtime: unsupported in this slice; REST snapshots and readback are the reconciliation path.
- Payment, wallet, withdraw, deposit, sub-account transfer, and fiat-transfer APIs: explicitly unsupported and outside this adapter's credential boundary.

## Philippines Spot Boundary

Coins.ph lists multiple quote assets, but this adapter is intentionally scoped to PHP spot markets for the Philippines venue slice. `exchangeInfo` parsing keeps only `quoteAsset == PHP`, and request validation rejects non-PHP `SymbolScope` values before network I/O.

## Binance-Like Versus Local API Audit

Coins.ph order/account payloads are Binance-like: `symbol`, `side`, `type`, `timeInForce`, `quantity`, `price`, `newClientOrderId`, `orderId`, `origClientOrderId`, account `balances`, order status, and `myTrades` fill fields follow the familiar Spot shape.

The adapter does not reuse or alias Binance:

- Adapter id is `coinsph`.
- Default host is `https://api.pro.coins.ph`.
- Signing header is `X-COINS-APIKEY`, not `X-MBX-APIKEY`.
- Trading/account paths use `/openapi/v1/...`, not `/api/v3/...`.
- Public depth uses the local quote namespace `/openapi/quote/v1/depth`.
- Environment variables are only `COINSPH_API_KEY`, `COINSPH_API_SECRET`, `COINSPH_RECV_WINDOW_MS`, and `COINSPH_PRIVATE_REST_ENABLED`.

## Signing And Fixtures

Signed REST uses HMAC-SHA256 hex over the canonical query string plus `timestamp`, `recvWindow`, and `signature`. Tests include the official BTCPHP query example and a local signing vector fixture:

- `tests/fixtures/exchanges/coinsph/signing_vectors/place_order_limit.json`
- `tests/fixtures/exchanges/coinsph/request_specs/`
- `tests/fixtures/exchanges/coinsph/parser/`

Request specs assert Coins.ph paths, `X-COINS-APIKEY`, `Content-Type`, signed query fields, and secret redaction.

## Unsupported Funding Boundary

The adapter only advertises `read_only` and `trade` credential scopes when private REST credentials are present. `transfer` and `withdraw` scopes remain unsupported. Coins.ph wallet, payment, crypto-account, deposit, withdraw, sub-account transfer, and fiat-transfer endpoints are documented as out of scope because trading automation must not require funding credentials.

## Current Limitations

- No quote-sized market order, cancel-all, amend, order-list, batch, payment, wallet, or fiat-transfer behavior.
- No WebSocket runtime.
- Cancel-all is left unsupported until Coins.ph local partial-cancel semantics are separately live-audited.
