# Bitunix Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + USDT perpetual REST adapter with WebSocket request-spec/parser coverage.

## Scope

- Adapter id: `bitunix`
- Spot REST: `https://openapi.bitunix.com`
- Futures REST: `https://fapi.bitunix.com`
- Market types: `Spot`, `Perpetual`
- WebSocket: Spot public request-response specs, USDT perpetual public/private subscription specs, login, heartbeat and parser helpers. The shared supervisor still owns live socket connection/reconnect orchestration.
- Sandbox: no stable official sandbox URL confirmed; base URLs are configurable

## Endpoint Mapping

| Gateway capability | Bitunix endpoint | Notes |
| --- | --- | --- |
| Spot symbol rules | `GET /api/spot/v1/common/coin_pair/list` | Parses compact symbols, base/quote assets, precision, min price/volume and trading status. |
| Perp symbol rules | `GET /api/v1/futures/market/trading_pairs` | Maps USDT-M symbols such as `BTCUSDT` to `MarketType::Perpetual`. |
| Spot order book | `GET /api/spot/v1/market/depth` | Parses object price/volume levels and exchange timestamp. |
| Perp order book | `GET /api/v1/futures/market/depth` | Parses array depth levels and normalizes supported depth values. |
| Spot balances | `GET /api/spot/v1/user/account` | Parses coin, available/free, frozen/locked and total balances. |
| Perp balances | `GET /api/v1/futures/account` | Requests `marginCoin=USDT` and parses margin account balance fields. |
| Perp positions | `GET /api/v1/futures/position/get_pending_positions` | Parses symbol, side, quantity, entry, liquidation, unrealized PnL and leverage. |
| Fee rate | Gateway fallback | Bitunix public/private fee endpoint was not verified; returns zero-rate snapshots with a source marker. |
| Spot order lifecycle | `POST /api/spot/v1/order/place_order`, `POST /api/spot/v1/order/cancel`, `GET /api/spot/v1/order/detail`, `POST /api/spot/v1/order/pending/list` | Limit/market order mapping with Spot numeric `side/type` fields, cancel, composed cancel-all, query and open orders. |
| Spot quote market | `POST /api/spot/v1/order/place_order` | BUY quote-sized market orders map quote notional to `amount`; Spot SELL and perpetual quote-sized market orders return `Unsupported`. |
| Spot batch orders | `POST /api/spot/v1/order/place_order/batch`, `POST /api/spot/v1/order/cancel` | Batch place and exchange-order-id batch cancel. |
| Perp order lifecycle | `POST /api/v1/futures/trade/place_order`, `POST /api/v1/futures/trade/modify_order`, `POST /api/v1/futures/trade/cancel_orders`, `POST /api/v1/futures/trade/cancel_all_orders`, `GET /api/v1/futures/trade/get_order_detail`, `GET /api/v1/futures/trade/get_pending_orders` | Limit/market/IOC/FOK/post-only, reduce-only mapped to close semantics, amend quantity, cancel, cancel-all, query and open orders. |
| Perp batch orders | `POST /api/v1/futures/trade/batch_order`, `POST /api/v1/futures/trade/cancel_orders` | Batch place and batch cancel for one symbol per request. |
| Fills | `POST /api/spot/v1/order/deal/list`, `GET /api/v1/futures/trade/get_history_trades` | Spot fills require an order id; perp fills support recent history filters. |
| Spot public WebSocket | `wss://openapi.bitunix.com:443/ws-api/v1` methods `market.depth`, `market.last_price`, `market.kline` | Authenticated request-response payload builder with Bitunix signing and ping metadata; public trades are not exposed through this Spot WS helper. |
| Futures public WebSocket | `wss://fapi.bitunix.com/public/` channels `depth_books`, `depth_book15`, `trade`, `ticker`, `market_kline_*` | Subscribe/unsubscribe specs, 15s client ping heartbeat metadata, heartbeat parser and order-book event normalization. |
| Futures private WebSocket | `wss://fapi.bitunix.com/private/` `login`, `order`, `balance`, `position` | Login payload signing, subscription specs, heartbeat parser, order/fill/balance/position standard stream event conversion. |

## Authentication

Private REST uses the official Bitunix header authentication:

```text
api-key: API key
nonce: caller nonce
timestamp: milliseconds
sign: SHA256(SHA256(nonce + timestamp + apiKey + sortedQuery + compactBody) + secret)
Content-Type: application/json
```

The adapter sorts query parameters by key and signs `key + value` pairs, matching the current futures documentation examples. Spot documentation has an inconsistent `key=value` example, so this should be live-dry-run validated before enabling private Spot trading.

## Unsupported / Boundary

- Standalone WebSocket runtime loop; the shared gateway supervisor is expected to consume the emitted specs and handle live connection/reconnect.
- Spot private WebSocket stream.
- Spot client order id, post-only/IOC/FOK, amend order, OCO/order-list.
- Spot sell quote-sized market and perpetual quote-sized market orders.
- General Spot fill history without an order id.
- Verified private fee-rate endpoint.

Use REST reconciliation fallback for unsupported Spot private stream state until the shared WebSocket supervisor is wired for Bitunix and live-dry-run validated.

## Task 20 Toolchain Status

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/bitunix/endpoint_mapping.yaml`.
- Capabilities v2: `toolchain.rs` declares Spot/perpetual public REST, gated private REST, REST-fallback WS runtime, native partial batch place/cancel, REST reconciliation, credential scopes and history limits.
- Fixtures: `tests/fixtures/exchanges/bitunix/` covers success, empty response, error response and missing required fields; public parser tests read fixture files directly.
- Request-spec/signing: private tests assert signed request method/path/header/body behavior; `bitunix_signing_should_match_double_sha256_vector` covers the double-SHA256 signing vector.
- WS policy: futures public/private WS are spec/parser ready with 15s client ping and reconnect/resubscribe requirements; Spot private WS remains unsupported.
- Rate-limit/pagination/reconciliation/batch: endpoint mapping declares buckets and limit pagination; open-orders/detail/history REST is the reconciliation fallback. Batch is native but partial/non-atomic and constrained to one market type.
- Live boundary: do not rely on WS-only private state before read-only account/order/fill validation and live-dry-run reconciliation.

## Official WebSocket Order Book Detail

Official futures public WS supports `depth_books`, `depth_book1`,
`depth_book5`, and `depth_book15`: full/changed book, 1 level, 5 levels, and 15
levels. The reviewed docs do not provide a fixed millisecond push interval,
sequence field, or checksum. Spot WS is request-response under
`wss://openapi.bitunix.com:443/ws-api/v1`. Mapping should add futures depth
channels, 1/5/15 levels, 15s ping policy, no sequence/checksum, and REST depth
fallback. Source batch:
[WebSocket 官方核验 P6 补充交易所盘口细项](../WebSocket官方核验_P6_补充交易所盘口细项.md).
