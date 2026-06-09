# Tokocrypto Gateway Adapter

Tokocrypto is implemented as a spot-only Binance-family adapter. It uses
Tokocrypto hosts and Tokocrypto private `/open/v1` paths, not Binance.com.

## Scope

- Exchange id: `tokocrypto`
- Market types: spot only
- Public REST: enabled for symbol rules and order book snapshots
- Private REST runtime: credential-gated read-only order/fill readbacks
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
`tests/fixtures/exchanges/tokocrypto/signing_vectors/`. Runtime read-only
private REST fails closed unless `TOKOCRYPTO_PRIVATE_REST_ENABLED` plus
`TOKOCRYPTO_API_KEY`/`TOKOCRYPTO_API_SECRET` credentials are present.

## Boundaries

The adapter does not expose live private trading, futures/perpetuals, margin,
withdrawals, fiat rails, Binance global routing, or NextMe live routing.
Private write routes are represented as request-spec fixtures only. Private
read-only order/fill routes are implemented for `query_order`,
`get_open_orders`, and `get_recent_fills` using signed `GET /open/v1` requests.
`query_order` requires `exchange_order_id`; client-order-id lookup is not
promoted. Fill readback requires context tenant/account for attribution.

P6 official product-line verification found this as a Tokocrypto spot/Binance-
family surface; `exchangeInfo` has `spotTradingEnable=1` and
`marginTradingEnable=0`, and no standard futures/perpetual/options API was
verified. Standard contracts are `交易所不支持合约`.

## Official WebSocket Order Book Detail

P9 official verification confirms MBX public streams support partial book depth
and diff depth. Partial stream names are `<symbol>@depth<levels>` or
`<symbol>@depth<levels>@100ms`, with levels 5, 10, or 20 and update speed
1000ms or 100ms. Diff depth uses `<symbol>@depth` or `<symbol>@depth@100ms` and
has `U/u` update ids. Rebuild with REST `/api/v3/depth` and `lastUpdateId`.
`endpoint_mapping.yaml` records the partial/diff channels, depth levels,
1000ms/100ms cadences, `U/u` sequence fields, and REST snapshot fallback.

## Fee Boundary

交易所不支持当前费率接口 runtime：fee readback 仅保留 offline request-spec 边界，未提升为 runtime。
