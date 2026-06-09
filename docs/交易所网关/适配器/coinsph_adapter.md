# Coins.ph Gateway Adapter

Status: independent Philippines Spot REST adapter slice for Task 19.

## Scope

- Public REST: PHP spot symbol rules from `GET /openapi/v1/exchangeInfo` and order book snapshots from `GET /openapi/quote/v1/depth`.
- Private REST: balances, trade fees, place order, cancel order, query order, open orders, and recent fills.
- Public/private WebSocket runtime: 项目未实现 in this slice; REST snapshots and readback are the reconciliation path.
- Payment, wallet, withdraw, deposit, sub-account transfer, and fiat-transfer APIs: explicitly unsupported and outside this adapter's credential boundary.

## Philippines Spot Boundary

Coins.ph lists multiple quote assets, but this adapter is intentionally scoped to PHP spot markets for the Philippines venue slice. `exchangeInfo` parsing keeps only `quoteAsset == PHP`, and request validation rejects non-PHP `SymbolScope` values before network I/O.

## Official Product-Line Boundary

官方核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。Coins.ph 官方 API/帮助中心明确这是 Spot Trade API，当前官方资料未见标准 futures/perpetual/options。单交易所文档写 `交易所不支持合约`。

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

## Advanced Order Unsupported Boundary

P4 高级订单能力保持明确不支持，不提升 live runtime。当前 Coins.ph Spot profile 只有普通 order/cancel/readback；未核到可无损映射 shared gateway 的 in-place amend、OCO/OTO order-list、native batch place/cancel，`cancel_all_orders` 也需要本地 partial-cancel 语义 live audit 后才可启用。

可执行证据：

- `endpoint_mapping.yaml` 声明 `amend_order`、`place_order_list`、`batch_place_orders`、`batch_cancel_orders`、`cancel_all_orders` 为 `support: unsupported`。
- `tests/fixtures/exchanges/coinsph/unsupported_boundary.json` 固定 capability flags 和 runtime `Unsupported.operation` 名称。
- `private_tests.rs` 覆盖 capabilities flags、`capabilities_v2` unsupported endpoint、runtime guard。

## Official Public WebSocket Gap

官方 WS 细项核验见 [WebSocket 官方核验 P3 P2 公共 WS 缺口交易所](../WebSocket官方核验_P3_P2公共WS缺口交易所.md)。Coins.ph supports Binance-like public streams at
`wss://wsapi.pro.coins.ph/openapi/quote/stream`: `@bookTicker` is real-time,
partial depth supports 5/10/20/200 levels, 5/10/20 levels can use `@100ms`,
200 levels are 1000ms, and diff depth supports 100ms/1000ms. Local book rebuild
uses REST depth snapshot plus `U/u` update-id continuity. Current project status
is `项目未实现公共 WS 行情`, not `交易所不支持`; `endpoint_mapping.yaml`
declares `websocket.public.support: spec_only` with the channel/depth/resync
boundary.

## Current Limitations

- No quote-sized market order, cancel-all, amend, order-list, batch, payment, wallet, or fiat-transfer behavior.
- No WebSocket runtime.
- Cancel-all is left unsupported until Coins.ph local partial-cancel semantics are separately live-audited.
