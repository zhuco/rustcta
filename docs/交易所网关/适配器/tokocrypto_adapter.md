# Tokocrypto Gateway Adapter

Tokocrypto is implemented as a spot-only Binance-family adapter. It uses
Tokocrypto hosts and Tokocrypto private `/open/v1` paths, not Binance.com.

## Scope

- Exchange id: `tokocrypto`
- Market types: spot only
- Public REST: enabled for symbol rules and order book snapshots
- Private REST runtime: disabled
- WebSocket runtime: disabled; subscribe/auth payloads are captured as fixtures

## Official Profile

- General and private REST base: `https://www.tokocrypto.com`
- MBX market REST base: `https://www.tokocrypto.site/api/v3`
- NextMe market REST base: `https://cloudme-toko.2meta.app/api/v1`
- MBX public stream base: `wss://stream-cloud.tokocrypto.site/stream`
- User WS API: `wss://ws-api.tokocrypto.site/ws-api/v3`

Public symbol metadata uses `GET /open/v1/common/symbols`. MBX order book
snapshots use `GET /api/v3/depth` and compact symbols such as `BTCUSDT`.
Private order examples use underscore symbols such as `BTC_USDT`.

## Signing

Tokocrypto uses the MBX signing profile:

- Header: `X-MBX-APIKEY`
- Signature: HMAC-SHA256 hex digest
- Payload: query string plus request body parameters, excluding `signature`
- Signed requests include `timestamp`; `recvWindow` is capped by the venue

Offline signing vectors live under
`tests/fixtures/exchanges/tokocrypto/signing_vectors/`.

## Boundaries

The adapter does not expose live private trading, futures/perpetuals, margin,
withdrawals, fiat rails, Binance global routing, or NextMe live routing.
Private order/account routes are represented as request-spec fixtures only.
