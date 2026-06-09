# Bitstamp Gateway Adapter

Adapter id: `bitstamp`

Status: Task 11 spot-only adapter for Bitstamp V2. The adapter covers symbol rules, public order books, HMAC-SHA256 V2 private REST, spot order lifecycle, fills, public WebSocket subscriptions, and private WebSocket token session setup.

## Scope

- REST: `https://www.bitstamp.net`
- Public WebSocket: `wss://ws.bitstamp.net`
- Market types: `Spot`
- Derivatives/Contracts: 项目未实现。Bitstamp official API changelog contains derivatives public/private endpoints, funding-rate endpoints and derivatives trade-history support, so this must not be written as `交易所不支持合约`.
- Symbols: Bitstamp market symbols are lowercase pairs such as `btcusd`.
- Signing: Bitstamp V2 HMAC-SHA256 headers over method, host, path, query, content type, nonce, timestamp, version and form body.

## Endpoint Mapping

Machine-readable mapping:

`crates/rustcta-exchange-gateway/src/adapters/bitstamp/endpoint_mapping.yaml`

| Gateway capability | Bitstamp endpoint | Status |
| --- | --- | --- |
| Symbol rules | `GET /api/v2/markets/` | Native parser |
| Order book | `GET /api/v2/order_book/{market}/?group=1` | Native snapshot parser |
| Balances | `POST /api/v2/account_balances/` | Signed REST |
| Fees | `POST /api/v2/fees/trading/{market}/` | Signed REST |
| Place order | `POST /api/v2/buy|sell/{market}/`, `buy|sell/market/{market}/` | Signed REST |
| Cancel, cancel all, replace | `POST /api/v2/cancel_order/`, `cancel_all_orders/`, `replace_order/` | Signed REST |
| Query/open orders | `POST /api/v2/order_status/`, `open_orders/all/` | Signed REST |
| Recent fills | `POST /api/v2/user_transactions/{market}/` | Signed REST, symbol scoped |
| Public WS | `order_book_`, `diff_order_book_`, `live_trades_` channels | Payload helpers |
| Private WS | `POST /api/v2/websockets_token/` then `private-my_orders` or `private-user` | Session spec helper |

## Public WebSocket Order Book

Official WebSocket v2 is the public real-time feed at `wss://ws.bitstamp.net`.
The structured Bitstamp public book policy covers `order_book_{market}` snapshots
and `diff_order_book_{market}` diff updates. The official material does not
publish a fixed millisecond push interval, a WebSocket depth selector, public
sequence continuity, or a checksum field for these channels. Treat the feed as
best-effort delta, not as a strict low-latency book.

| Field | Bitstamp detail |
| --- | --- |
| Subscribe payload | `{"event":"bts:subscribe","data":{"channel":"order_book_btcusd"}}` or `diff_order_book_btcusd`. |
| Snapshot channel | `order_book_{market_symbol}`. |
| Diff channel | `diff_order_book_{market_symbol}`. |
| Interval/depth | Real-time push; no fixed ms or WebSocket depth selector documented. |
| Sequence/checksum | No public sequence or checksum contract documented for these channels. |
| REST snapshot | `GET /api/v2/order_book/{market_symbol}/`, default grouped price levels with `group=1`. |
| Gap recovery | `POST /api/v2/order_data/` with `market`, `since_id`, and `until_id` for public order event recovery. |
| Resync policy | Rebuild from REST `order_book`, use `order_data` when an event id window is available, and reconnect/rebuild on stale stream, suspected message loss, or unavailable recovery window. |

## Unsupported Boundaries

- Derivatives, margin positions and contract metadata are 项目未实现 in this spot adapter.
- Sell quote-sized market orders, native order lists and batch place are `Unsupported`.
- Private WebSocket positions are `Unsupported` because Bitstamp spot has no shared position model.

2026-06-09 产品线边界收窄：`contract_product` 与
`derivatives_product` 绑定
`tests/fixtures/exchanges/bitstamp/request_specs/product_line_source_boundary.json`。
Bitstamp Derivatives/Contracts 继续写 `项目未实现`，不是 `交易所不支持合约`；
当前 Spot adapter 的 balances/orders/WS 不复用到 derivatives profile，剩余工作是
derivatives metadata、positions/margin/funding、signed order lifecycle 和 dry-run guard。
状态建议：继续保留 `contract_product` / `derivatives_product = 项目未实现`；官方
Derivatives/Contracts API 线索不能归类为交易所不支持，也不能由 Spot 私有读写或
Spot WS 直接外推为 derivatives runtime。

## Validation

Recommended targeted validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitstamp/endpoint_mapping.yaml
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/bitstamp/*.rs
cargo test -p rustcta-exchange-gateway bitstamp_signing --lib --message-format short
cargo test -p rustcta-exchange-gateway bitstamp_private_parsers --lib --message-format short
cargo test -p rustcta-exchange-gateway bitstamp_private_ws_payload --lib --message-format short
```

Do not enable private REST or private streams until API scopes, subaccount routing and trading limits are verified.
